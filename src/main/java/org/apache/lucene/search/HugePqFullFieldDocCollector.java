/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search;

import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.FieldValueHitQueue;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLFieldDoc;
import it.cavallium.dbengine.lucene.LLSlotDoc;
import it.cavallium.dbengine.lucene.LLSlotDocCodec;
import it.cavallium.dbengine.lucene.HugePqPriorityQueue;
import it.cavallium.dbengine.lucene.MaxScoreAccumulator;
import it.cavallium.dbengine.lucene.PriorityQueue;
import it.cavallium.dbengine.lucene.ResourceIterable;
import it.cavallium.dbengine.lucene.collector.FullDocsCollector;
import it.cavallium.dbengine.lucene.collector.FullFieldDocs;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.TotalHits.Relation;
import reactor.core.publisher.Flux;

/**
 * A {@link org.apache.lucene.search.Collector} that sorts by {@link SortField} using {@link FieldComparator}s.
 *
 * <p>See the {@link #create(LLTempHugePqEnv, Sort, int, int)} (org.apache.lucene.search.Sort, int, int)} method for instantiating a
 * TopFieldCollector.
 *
 * This class must mirror this changes:
 * <a href="https://github.com/apache/lucene/commits/main/lucene/core/src/java/org/apache/lucene/search/TopFieldCollector.java">
 *   Lucene TopFieldCollector changes on GitHub</a>
 */
public abstract class HugePqFullFieldDocCollector extends
		FullDocsCollector<HugePqPriorityQueue<LLSlotDoc>, LLSlotDoc, LLFieldDoc> {

	// TODO: one optimization we could do is to pre-fill
	// the queue with sentinel value that guaranteed to
	// always compare lower than a real hit; this would
	// save having to check queueFull on each insert

	private abstract class TopFieldLeafCollector implements LeafCollector {

		final LeafFieldComparator comparator;
		final int reverseMul;
		Scorable scorer;
		boolean collectedAllCompetitiveHits = false;

		TopFieldLeafCollector(FieldValueHitQueue fieldValueHitQueue, Sort sort, LeafReaderContext context)
				throws IOException {
			// as all segments are sorted in the same way, enough to check only the 1st segment for
			// indexSort
			if (searchSortPartOfIndexSort == null) {
				final Sort indexSort = context.reader().getMetaData().getSort();
				searchSortPartOfIndexSort = canEarlyTerminate(sort, indexSort);
				if (searchSortPartOfIndexSort) {
					firstComparator.disableSkipping();
				}
			}
			LeafFieldComparator[] comparators = fieldValueHitQueue.getComparators(context);
			int[] reverseMuls = fieldValueHitQueue.getReverseMul();
			if (comparators.length == 1) {
				this.reverseMul = reverseMuls[0];
				this.comparator = comparators[0];
			} else {
				this.reverseMul = 1;
				this.comparator = new MultiLeafFieldComparator(comparators, reverseMuls);
			}
		}

		void countHit(int doc) throws IOException {
			++totalHits;
			hitsThresholdChecker.incrementHitCount();

			if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
				updateGlobalMinCompetitiveScore(scorer);
			}
			if (!scoreMode.isExhaustive()
					&& totalHitsRelation == TotalHits.Relation.EQUAL_TO
					&& hitsThresholdChecker.isThresholdReached()) {
				// for the first time hitsThreshold is reached, notify comparator about this
				comparator.setHitsThresholdReached();
				totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
			}
		}

		boolean thresholdCheck(int doc) throws IOException {
			if (collectedAllCompetitiveHits || reverseMul * comparator.compareBottom(doc) <= 0) {
				// since docs are visited in doc Id order, if compare is 0, it means
				// this document is largest than anything else in the queue, and
				// therefore not competitive.
				if (searchSortPartOfIndexSort) {
					if (hitsThresholdChecker.isThresholdReached()) {
						totalHitsRelation = Relation.GREATER_THAN_OR_EQUAL_TO;
						throw new CollectionTerminatedException();
					} else {
						collectedAllCompetitiveHits = true;
					}
				} else if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
					// we can start setting the min competitive score if the
					// threshold is reached for the first time here.
					updateMinCompetitiveScore(scorer);
				}
				return true;
			}
			return false;
		}

		void collectCompetitiveHit(int doc) throws IOException {
			// This hit is competitive - replace bottom element in queue & adjustTop
			comparator.copy(pq.top().slot(), doc);
			updateBottom(doc);
			comparator.setBottom(pq.top().slot());
			updateMinCompetitiveScore(scorer);
		}

		void collectAnyHit(int doc, int hitsCollected) throws IOException {
			// Startup transient: queue hasn't gathered numHits yet
			int slot = hitsCollected - 1;
			// Copy hit into queue
			comparator.copy(slot, doc);
			add(slot, doc);
			if (queueFull) {
				comparator.setBottom(pq.top().slot());
				updateMinCompetitiveScore(scorer);
			}
		}

		@Override
		public void setScorer(Scorable scorer) throws IOException {
			this.scorer = scorer;
			comparator.setScorer(scorer);
			minCompetitiveScore = 0f;
			updateMinCompetitiveScore(scorer);
			if (minScoreAcc != null) {
				updateGlobalMinCompetitiveScore(scorer);
			}
		}

		@Override
		public DocIdSetIterator competitiveIterator() throws IOException {
			return comparator.competitiveIterator();
		}
	}

	static boolean canEarlyTerminate(Sort searchSort, Sort indexSort) {
		return canEarlyTerminateOnDocId(searchSort) || canEarlyTerminateOnPrefix(searchSort, indexSort);
	}

	private static boolean canEarlyTerminateOnDocId(Sort searchSort) {
		final SortField[] fields1 = searchSort.getSort();
		return SortField.FIELD_DOC.equals(fields1[0]);
	}

	private static boolean canEarlyTerminateOnPrefix(Sort searchSort, Sort indexSort) {
		if (indexSort != null) {
			final SortField[] fields1 = searchSort.getSort();
			final SortField[] fields2 = indexSort.getSort();
			// early termination is possible if fields1 is a prefix of fields2
			if (fields1.length > fields2.length) {
				return false;
			}
			return Arrays.asList(fields1).equals(Arrays.asList(fields2).subList(0, fields1.length));
		} else {
			return false;
		}
	}

	/*
	 * Implements a TopFieldCollector over one SortField criteria, with tracking
	 * document scores and maxScore.
	 */
	private static class SimpleFieldCollector extends HugePqFullFieldDocCollector {
		final Sort sort;
		final PriorityQueue<LLSlotDoc> queue;
		private final FieldValueHitQueue fieldValueHitQueue;

		public SimpleFieldCollector(
				Sort sort,
				HugePqPriorityQueue<LLSlotDoc> queue,
				FieldValueHitQueue fieldValueHitQueue,
				long numHits,
				HitsThresholdChecker hitsThresholdChecker,
				MaxScoreAccumulator minScoreAcc) {
			super(queue, fieldValueHitQueue, numHits, hitsThresholdChecker, sort.needsScores(), minScoreAcc);
			this.sort = sort;
			this.queue = queue;
			this.fieldValueHitQueue = fieldValueHitQueue;
		}

		@Override
		public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
			docBase = context.docBase;

			return new TopFieldLeafCollector(fieldValueHitQueue, sort, context) {

				@Override
				public void collect(int doc) throws IOException {
					countHit(doc);
					if (queueFull) {
						if (thresholdCheck(doc)) {
							return;
						}
						collectCompetitiveHit(doc);
					} else {
						collectAnyHit(doc, totalHits);
					}
				}
			};
		}

		@Override
		public ResourceIterable<LLFieldDoc> mapResults(ResourceIterable<LLSlotDoc> it) {
			return new ResourceIterable<>() {
				@Override
				public void close() {
					it.close();
				}

				@Override
				public Flux<LLFieldDoc> iterate() {
					return it.iterate().map(fieldValueHitQueue::fillFields);
				}

				@Override
				public Flux<LLFieldDoc> iterate(long skips) {
					return it.iterate(skips).map(fieldValueHitQueue::fillFields);
				}
			};
		}
	}

	final long numHits;
	final HitsThresholdChecker hitsThresholdChecker;
	final FieldComparator<?> firstComparator;
	final boolean canSetMinScore;

	Boolean searchSortPartOfIndexSort = null; // shows if Search Sort if a part of the Index Sort

	// an accumulator that maintains the maximum of the segment's minimum competitive scores
	final MaxScoreAccumulator minScoreAcc;
	// the current local minimum competitive score already propagated to the underlying scorer
	float minCompetitiveScore;

	final int numComparators;
	boolean queueFull;
	int docBase;
	final boolean needsScores;
	final ScoreMode scoreMode;

	// Declaring the constructor private prevents extending this class by anyone
	// else. Note that the class cannot be final since it's extended by the
	// internal versions. If someone will define a constructor with any other
	// visibility, then anyone will be able to extend the class, which is not what
	// we want.
	private HugePqFullFieldDocCollector(
			HugePqPriorityQueue<LLSlotDoc> pq,
			FieldValueHitQueue fieldValueHitQueue,
			long numHits,
			HitsThresholdChecker hitsThresholdChecker,
			boolean needsScores,
			MaxScoreAccumulator minScoreAcc) {
		super(pq);
		this.needsScores = needsScores;
		this.numHits = numHits;
		this.hitsThresholdChecker = hitsThresholdChecker;
		this.numComparators = fieldValueHitQueue.getComparators().length;
		this.firstComparator = fieldValueHitQueue.getComparators()[0];
		int reverseMul = fieldValueHitQueue.getReverseMul()[0];

		if (firstComparator.getClass().equals(FieldComparator.RelevanceComparator.class)
				&& reverseMul == 1 // if the natural sort is preserved (sort by descending relevance)
				&& hitsThresholdChecker.getHitsThreshold() != Integer.MAX_VALUE) {
			scoreMode = ScoreMode.TOP_SCORES;
			canSetMinScore = true;
		} else {
			canSetMinScore = false;
			if (hitsThresholdChecker.getHitsThreshold() != Integer.MAX_VALUE) {
				scoreMode = needsScores ? ScoreMode.TOP_DOCS_WITH_SCORES : ScoreMode.TOP_DOCS;
			} else {
				scoreMode = needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
			}
		}
		this.minScoreAcc = minScoreAcc;
	}

	@Override
	public ScoreMode scoreMode() {
		return scoreMode;
	}

	protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
		assert minScoreAcc != null;
		if (canSetMinScore && hitsThresholdChecker.isThresholdReached()) {
			// we can start checking the global maximum score even
			// if the local queue is not full because the threshold
			// is reached.
			MaxScoreAccumulator.DocAndScore maxMinScore = minScoreAcc.get();
			if (maxMinScore != null && maxMinScore.score > minCompetitiveScore) {
				scorer.setMinCompetitiveScore(maxMinScore.score);
				minCompetitiveScore = maxMinScore.score;
				totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
			}
		}
	}

	protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
		if (canSetMinScore && queueFull && hitsThresholdChecker.isThresholdReached()) {
			assert pq.top() != null;
			float minScore = (float) firstComparator.value(pq.top().slot());
			if (minScore > minCompetitiveScore) {
				scorer.setMinCompetitiveScore(minScore);
				minCompetitiveScore = minScore;
				totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
				if (minScoreAcc != null) {
					minScoreAcc.accumulate(pq.top().doc(), minScore);
				}
			}
		}
	}

	/**
	 * Creates a new {@link HugePqFullFieldDocCollector} from the given arguments.
	 *
	 * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
	 * <code>numHits</code>.
	 *
	 * @param sort the sort criteria (SortFields).
	 * @param numHits the number of results to collect.
	 * @param totalHitsThreshold the number of docs to count accurately. If the query matches more
	 *     than {@code totalHitsThreshold} hits then its hit count will be a lower bound. On the other
	 *     hand if the query matches less than or exactly {@code totalHitsThreshold} hits then the hit
	 *     count of the result will be accurate. {@link Integer#MAX_VALUE} may be used to make the hit
	 *     count accurate, but this will also make query processing slower.
	 * @return a {@link HugePqFullFieldDocCollector} instance which will sort the results by the sort criteria.
	 */
	public static HugePqFullFieldDocCollector create(LLTempHugePqEnv env, Sort sort, int numHits, int totalHitsThreshold) {
		if (totalHitsThreshold < 0) {
			throw new IllegalArgumentException(
					"totalHitsThreshold must be >= 0, got " + totalHitsThreshold);
		}

		return create(
				env,
				sort,
				numHits,
				HitsThresholdChecker.create(Math.max(totalHitsThreshold, numHits)),
				null /* bottomValueChecker */);
	}

	/**
	 * Same as above with additional parameters to allow passing in the threshold checker and the max
	 * score accumulator.
	 */
	static HugePqFullFieldDocCollector create(
			LLTempHugePqEnv env,
			Sort sort,
			int numHits,
			HitsThresholdChecker hitsThresholdChecker,
			MaxScoreAccumulator minScoreAcc) {

		if (sort.getSort().length == 0) {
			throw new IllegalArgumentException("Sort must contain at least one field");
		}

		if (numHits <= 0) {
			throw new IllegalArgumentException(
					"numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
		}

		if (hitsThresholdChecker == null) {
			throw new IllegalArgumentException("hitsThresholdChecker should not be null");
		}

		var fieldValueHitQueue = new LLSlotDocCodec(env, numHits, sort.getSort());
		var queue = new HugePqPriorityQueue<>(env, fieldValueHitQueue);

		// inform a comparator that sort is based on this single field
		// to enable some optimizations for skipping over non-competitive documents
		// We can't set single sort when the `after` parameter is non-null as it's
		// an implicit sort over the document id.
		if (fieldValueHitQueue.getComparators().length == 1) {
			fieldValueHitQueue.getComparators()[0].setSingleSort();
		}
		return new SimpleFieldCollector(sort, queue, fieldValueHitQueue, numHits, hitsThresholdChecker, minScoreAcc);
	}

	/**
	 * Create a CollectorManager which uses a shared hit counter to maintain number of hits and a
	 * shared {@link MaxScoreAccumulator} to propagate the minimum score accross segments if the
	 * primary sort is by relevancy.
	 */
	public static CollectorManager<HugePqFullFieldDocCollector, FullFieldDocs<LLFieldDoc>> createSharedManager(
			LLTempHugePqEnv env, Sort sort, int numHits, long totalHitsThreshold) {
		return new CollectorManager<>() {

			private final HitsThresholdChecker hitsThresholdChecker;

			{
				if (totalHitsThreshold < Integer.MAX_VALUE) {
					hitsThresholdChecker = HitsThresholdChecker.createShared(Math.max((int) totalHitsThreshold, numHits));
				} else {
					hitsThresholdChecker = HitsThresholdChecker.createShared(Integer.MAX_VALUE);
				}
			}

			private final MaxScoreAccumulator minScoreAcc = new MaxScoreAccumulator();

			@Override
			public HugePqFullFieldDocCollector newCollector() {
				return create(env, sort, numHits, hitsThresholdChecker, minScoreAcc);
			}

			@Override
			public FullFieldDocs<LLFieldDoc> reduce(Collection<HugePqFullFieldDocCollector> collectors) {
				return reduceShared(sort, collectors);
			}
		};
	}

	private static FullFieldDocs<LLFieldDoc> reduceShared(Sort sort, Collection<HugePqFullFieldDocCollector> collectors) {
		@SuppressWarnings("unchecked")
		final FullDocs<LLFieldDoc>[] fullDocs = new FullDocs[collectors.size()];
		int i = 0;
		for (var collector : collectors) {
			fullDocs[i++] = collector.fullDocs();
		}
		return (FullFieldDocs<LLFieldDoc>) FullDocs.merge(sort, fullDocs);

	}

	final void add(int slot, int doc) {
		pq.add(new LLSlotDoc(docBase + doc, Float.NaN, -1, slot));

		// The queue is full either when totalHits == numHits (in SimpleFieldCollector), in which case
		// slot = totalHits - 1, or when hitsCollected == numHits (in PagingFieldCollector this is hits
		// on the current page) and slot = hitsCollected - 1.
		assert slot < numHits;
		queueFull = slot == numHits - 1;
	}

	//todo: check if this part is efficient and not redundant
	final void updateBottom(int doc) {
		// bottom.score is already set to Float.NaN in add().
		var bottom = pq.top();
		pq.replaceTop(new LLSlotDoc(docBase + doc, bottom.score(), bottom.shardIndex(), bottom.slot()));
	}

	/*
	 * Only the following callback methods need to be overridden since
	 * topDocs(int, int) calls them to return the results.
	 */

	/** Return whether collection terminated early. */
	public boolean isEarlyTerminated() {
		return totalHitsRelation == Relation.GREATER_THAN_OR_EQUAL_TO;
	}

	@Override
	public void close() {
		this.pq.close();
		if (this.firstComparator instanceof SafeCloseable closeable) {
			closeable.close();
		}
		super.close();
	}
}
