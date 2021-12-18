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
package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLScoreDoc;
import it.cavallium.dbengine.lucene.LLScoreDocCodec;
import it.cavallium.dbengine.lucene.LMDBPriorityQueue;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.MaxScoreAccumulator;
import it.cavallium.dbengine.lucene.ResourceIterable;
import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import it.cavallium.dbengine.lucene.MaxScoreAccumulator.DocAndScore;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHits;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link Collector} implementation that collects the top-scoring hits, returning them as a {@link
 * FullDocs}. This is used by {@link IndexSearcher} to implement {@link FullDocs}-based search. Hits
 * are sorted by score descending and then (when the scores are tied) docID ascending. When you
 * create an instance of this collector you should know in advance whether documents are going to be
 * collected in doc Id order or not.
 *
 * <p><b>NOTE</b>: The values {@link Float#NaN} and {@link Float#NEGATIVE_INFINITY} are not valid
 * scores. This collector will not properly collect hits with such scores.
 *
 * This class must mirror this changes:
 * <a href="https://github.com/apache/lucene/commits/main/lucene/core/src/java/org/apache/lucene/search/TopScoreDocCollector.java">
 *   Lucene TopScoreDocCollector changes on GitHub</a>
 */
public abstract class LMDBFullScoreDocCollector extends FullDocsCollector<LMDBPriorityQueue<LLScoreDoc>, LLScoreDoc, LLScoreDoc> {

	/** Scorable leaf collector */
	public abstract static class ScorerLeafCollector implements LeafCollector {

		protected Scorable scorer;

		@Override
		public void setScorer(Scorable scorer) throws IOException {
			this.scorer = scorer;
		}
	}

	private static class SimpleLMDBFullScoreDocCollector extends LMDBFullScoreDocCollector {

		SimpleLMDBFullScoreDocCollector(LLTempLMDBEnv env, @Nullable Long limit,
				HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
			super(env, limit, hitsThresholdChecker, minScoreAcc);
		}

		@Override
		public LeafCollector getLeafCollector(LeafReaderContext context) {
			// reset the minimum competitive score
			docBase = context.docBase;
			minCompetitiveScore = 0f;
			return new ScorerLeafCollector() {

				@Override
				public void setScorer(Scorable scorer) throws IOException {
					super.setScorer(scorer);
					if (minScoreAcc == null) {
						updateMinCompetitiveScore(scorer);
					} else {
						updateGlobalMinCompetitiveScore(scorer);
					}
				}

				@Override
				public void collect(int doc) throws IOException {
					float score = scorer.score();

					// This collector relies on the fact that scorers produce positive values:
					assert score >= 0; // NOTE: false for NaN

					totalHits++;
					hitsThresholdChecker.incrementHitCount();

					if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
						updateGlobalMinCompetitiveScore(scorer);
					}

					// If there is a limit, and it's reached, use the replacement logic
					if (limit != null && pq.size() >= limit) {
						var pqTop = pq.top();
						if (pqTop != null && score <= pqTop.score()) {
							if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
								// we just reached totalHitsThreshold, we can start setting the min
								// competitive score now
								updateMinCompetitiveScore(scorer);
							}
							// Since docs are returned in-order (i.e., increasing doc Id), a document
							// with equal score to pqTop.score cannot compete since HitQueue favors
							// documents with lower doc Ids. Therefore reject those docs too.
							return;
						} else {
							// Remove the top element, then add the following element
							pq.replaceTop(new LLScoreDoc(doc + docBase, score, -1));
							// The minimum competitive score will be updated later
						}
					} else {
						// There is no limit or the limit has not been reached. Add the document to the queue
						pq.add(new LLScoreDoc(doc + docBase, score, -1));
						// The minimum competitive score will be updated later
					}
					// Update the minimum competitive score
					updateMinCompetitiveScore(scorer);
				}
			};
		}

		@Override
		public ResourceIterable<LLScoreDoc> mapResults(ResourceIterable<LLScoreDoc> it) {
			return it;
		}
	}

	/**
	 * Creates a new {@link LMDBFullScoreDocCollector} given the number of hits to collect and the number
	 * of hits to count accurately.
	 *
	 * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly {@code
	 * totalHitsThreshold} then this value is accurate. On the other hand, if the {@link
	 * FullDocs#totalHits} value is greater than {@code totalHitsThreshold} then its value is a lower
	 * bound of the hit count. A value of {@link Integer#MAX_VALUE} will make the hit count accurate
	 * but will also likely make query processing slower.
	 *
	 * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
	 * <code>numHits</code>, and fill the array with sentinel objects.
	 */
	public static LMDBFullScoreDocCollector create(LLTempLMDBEnv env, long numHits, int totalHitsThreshold) {
		return create(env, numHits, HitsThresholdChecker.create(totalHitsThreshold), null);
	}

	/**
	 * Creates a new {@link LMDBFullScoreDocCollector} given the number of hits to count accurately.
	 *
	 * <p><b>NOTE</b>:  A value of {@link Integer#MAX_VALUE} will make the hit count accurate
	 * but will also likely make query processing slower.
	 */
	public static LMDBFullScoreDocCollector create(LLTempLMDBEnv env, int totalHitsThreshold) {
		return create(env, HitsThresholdChecker.create(totalHitsThreshold), null);
	}

	static LMDBFullScoreDocCollector create(
			LLTempLMDBEnv env,
			HitsThresholdChecker hitsThresholdChecker,
			MaxScoreAccumulator minScoreAcc) {

		if (hitsThresholdChecker == null) {
			throw new IllegalArgumentException("hitsThresholdChecker must be non null");
		}

		return new SimpleLMDBFullScoreDocCollector(env, null, hitsThresholdChecker, minScoreAcc);
	}

	static LMDBFullScoreDocCollector create(
			LLTempLMDBEnv env,
			@NotNull Long numHits,
			HitsThresholdChecker hitsThresholdChecker,
			MaxScoreAccumulator minScoreAcc) {

		if (hitsThresholdChecker == null) {
			throw new IllegalArgumentException("hitsThresholdChecker must be non null");
		}

		return new SimpleLMDBFullScoreDocCollector(env,
				(numHits < 0 || numHits >= 2147483630L) ? null : numHits,
				hitsThresholdChecker,
				minScoreAcc
		);
	}

	/**
	 * Create a CollectorManager which uses a shared hit counter to maintain number of hits and a
	 * shared {@link MaxScoreAccumulator} to propagate the minimum score accross segments
	 */
	public static CollectorManager<LMDBFullScoreDocCollector, FullDocs<LLScoreDoc>> createSharedManager(
			LLTempLMDBEnv env,
			long numHits,
			long totalHitsThreshold) {
		return new CollectorManager<>() {

			private final HitsThresholdChecker hitsThresholdChecker =
					HitsThresholdChecker.createShared(totalHitsThreshold);
			private final MaxScoreAccumulator minScoreAcc = new MaxScoreAccumulator();

			@Override
			public LMDBFullScoreDocCollector newCollector() {
				return LMDBFullScoreDocCollector.create(env, numHits, hitsThresholdChecker, minScoreAcc);
			}

			@Override
			public FullDocs<LLScoreDoc> reduce(Collection<LMDBFullScoreDocCollector> collectors) {
				return reduceShared(collectors);
			}
		};
	}

	/**
	 * Create a CollectorManager which uses a shared {@link MaxScoreAccumulator} to propagate
	 * the minimum score accross segments
	 */
	public static CollectorManager<LMDBFullScoreDocCollector, FullDocs<LLScoreDoc>> createSharedManager(
			LLTempLMDBEnv env,
			long totalHitsThreshold) {
		return new CollectorManager<>() {

			private final HitsThresholdChecker hitsThresholdChecker =
					HitsThresholdChecker.createShared(totalHitsThreshold);
			private final MaxScoreAccumulator minScoreAcc = new MaxScoreAccumulator();

			@Override
			public LMDBFullScoreDocCollector newCollector() {
				return LMDBFullScoreDocCollector.create(env, hitsThresholdChecker, minScoreAcc);
			}

			@Override
			public FullDocs<LLScoreDoc> reduce(Collection<LMDBFullScoreDocCollector> collectors) {
				return reduceShared(collectors);
			}
		};
	}

	private static FullDocs<LLScoreDoc> reduceShared(Collection<LMDBFullScoreDocCollector> collectors) {
		@SuppressWarnings("unchecked")
		final FullDocs<LLScoreDoc>[] fullDocs = new FullDocs[collectors.size()];
		int i = 0;
		for (LMDBFullScoreDocCollector collector : collectors) {
			fullDocs[i++] = collector.fullDocs();
		}
		return FullDocs.merge(null, fullDocs);
	}

	int docBase;
	final @Nullable Long limit;
	final HitsThresholdChecker hitsThresholdChecker;
	final MaxScoreAccumulator minScoreAcc;
	float minCompetitiveScore;

	// prevents instantiation
	LMDBFullScoreDocCollector(LLTempLMDBEnv env, @Nullable Long limit,
			HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
		super(new LMDBPriorityQueue<>(env, new LLScoreDocCodec()));
		assert hitsThresholdChecker != null;
		this.limit = limit;
		this.hitsThresholdChecker = hitsThresholdChecker;
		this.minScoreAcc = minScoreAcc;
	}

	@Override
	public ScoreMode scoreMode() {
		return hitsThresholdChecker.scoreMode();
	}

	protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
		assert minScoreAcc != null;
		DocAndScore maxMinScore = minScoreAcc.get();
		if (maxMinScore != null) {
			// since we tie-break on doc id and collect in doc id order we can require
			// the next float if the global minimum score is set on a document id that is
			// smaller than the ids in the current leaf
			float score =
					docBase >= maxMinScore.docBase ? Math.nextUp(maxMinScore.score) : maxMinScore.score;
			if (score > minCompetitiveScore) {
				assert hitsThresholdChecker.isThresholdReached(true);
				scorer.setMinCompetitiveScore(score);
				minCompetitiveScore = score;
				totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
			}
		}
	}

	protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
		var pqTop = pq.top();
		if (hitsThresholdChecker.isThresholdReached(true)
				&& pqTop != null
				&& pqTop.score() != Float.NEGATIVE_INFINITY) { // -Infinity is the score of sentinels
			// since we tie-break on doc id and collect in doc id order, we can require
			// the next float
			float localMinScore = Math.nextUp(pqTop.score());
			if (localMinScore > minCompetitiveScore) {
				scorer.setMinCompetitiveScore(localMinScore);
				totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
				minCompetitiveScore = localMinScore;
				if (minScoreAcc != null) {
					// we don't use the next float but we register the document
					// id so that other leaves can require it if they are after
					// the current maximum
					minScoreAcc.accumulate(docBase, pqTop.score());
				}
			}
		}
	}

}