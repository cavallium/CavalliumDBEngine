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

import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.FieldValueHitQueue;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLDocElement;
import it.cavallium.dbengine.lucene.LLScoreDoc;
import it.cavallium.dbengine.lucene.LLSlotDoc;
import it.cavallium.dbengine.lucene.LLSlotDocCodec;
import it.cavallium.dbengine.lucene.LMDBPriorityQueue;
import it.cavallium.dbengine.lucene.MaxScoreAccumulator;
import it.cavallium.dbengine.lucene.MaxScoreAccumulator.DocAndScore;
import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class LMDBFullFieldDocCollector extends FullDocsCollector<LLDocElement> {

	private final FieldValueHitQueue fieldValueHitQueue;

	public abstract static class ScorerLeafCollector implements LeafCollector {

		protected Scorable scorer;

		@Override
		public void setScorer(Scorable scorer) throws IOException {
			this.scorer = scorer;
		}
	}

	private static class SimpleLMDBFullScoreDocCollector extends LMDBFullFieldDocCollector {

		SimpleLMDBFullScoreDocCollector(LLTempLMDBEnv env, @Nullable Long limit,
				HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
			super(env, limit, hitsThresholdChecker, minScoreAcc);
		}

		@Override
		public LeafCollector getLeafCollector(LeafReaderContext context) {
			docBase = context.docBase;
			return new ScorerLeafCollector() {

				@Override
				public void setScorer(Scorable scorer) throws IOException {
					super.setScorer(scorer);
					minCompetitiveScore = 0f;
					updateMinCompetitiveScore(scorer);
					if (minScoreAcc != null) {
						updateGlobalMinCompetitiveScore(scorer);
					}
				}

				@Override
				public void collect(int doc) throws IOException {
					float score = scorer.score();

					assert score >= 0;

					totalHits++;
					hitsThresholdChecker.incrementHitCount();

					if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
						updateGlobalMinCompetitiveScore(scorer);
					}

					if (limit != null && pq.size() >= limit) {
						var pqTop = pq.top();
						if (pqTop != null && score <= pqTop.score()) {
							if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
								updateMinCompetitiveScore(scorer);
							}
							return;
						} else {
							pq.replaceTop(new LLScoreDoc(doc + docBase, score, -1));
						}
					} else {
						pq.add(new LLScoreDoc(doc + docBase, score, -1));
					}
					updateMinCompetitiveScore(scorer);
				}
			};
		}
	}

	public static LMDBFullFieldDocCollector create(LLTempLMDBEnv env, long numHits, int totalHitsThreshold) {
		return create(env, numHits, HitsThresholdChecker.create(totalHitsThreshold), null);
	}

	public static LMDBFullFieldDocCollector create(LLTempLMDBEnv env, int totalHitsThreshold) {
		return create(env, HitsThresholdChecker.create(totalHitsThreshold), null);
	}

	static LMDBFullFieldDocCollector create(
			LLTempLMDBEnv env,
			HitsThresholdChecker hitsThresholdChecker,
			MaxScoreAccumulator minScoreAcc) {

		if (hitsThresholdChecker == null) {
			throw new IllegalArgumentException("hitsThresholdChecker must be non null");
		}

		return new SimpleLMDBFullScoreDocCollector(env, null, hitsThresholdChecker, minScoreAcc);
	}

	static LMDBFullFieldDocCollector create(
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

	public static CollectorManager<LMDBFullFieldDocCollector, FullDocs<LLScoreDoc>> createSharedManager(
			LLTempLMDBEnv env,
			long numHits,
			int totalHitsThreshold) {
		return new CollectorManager<>() {

			private final HitsThresholdChecker hitsThresholdChecker =
					HitsThresholdChecker.createShared(totalHitsThreshold);
			private final MaxScoreAccumulator minScoreAcc = new MaxScoreAccumulator();

			@Override
			public LMDBFullFieldDocCollector newCollector() {
				return LMDBFullFieldDocCollector.create(env, numHits, hitsThresholdChecker, minScoreAcc);
			}

			@Override
			public FullDocs<LLScoreDoc> reduce(Collection<LMDBFullFieldDocCollector> collectors) {
				return reduceShared(collectors);
			}
		};
	}

	public static CollectorManager<LMDBFullFieldDocCollector, FullDocs<LLScoreDoc>> createSharedManager(
			LLTempLMDBEnv env,
			int totalHitsThreshold) {
		return new CollectorManager<>() {

			private final HitsThresholdChecker hitsThresholdChecker =
					HitsThresholdChecker.createShared(totalHitsThreshold);
			private final MaxScoreAccumulator minScoreAcc = new MaxScoreAccumulator();

			@Override
			public LMDBFullFieldDocCollector newCollector() {
				return LMDBFullFieldDocCollector.create(env, hitsThresholdChecker, minScoreAcc);
			}

			@Override
			public FullDocs<LLScoreDoc> reduce(Collection<LMDBFullFieldDocCollector> collectors) {
				return reduceShared(collectors);
			}
		};
	}

	private static FullDocs<LLSlotDoc> reduceShared(Collection<LMDBFullFieldDocCollector> collectors) {
		@SuppressWarnings("unchecked")
		final FullDocs<LLSlotDoc>[] fullDocs = new FullDocs[collectors.size()];
		int i = 0;
		for (LMDBFullFieldDocCollector collector : collectors) {
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
	LMDBFullFieldDocCollector(LLTempLMDBEnv env, SortField[] fields, @Nullable Long limit,
			HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
		//noinspection unchecked
		super(new LMDBPriorityQueue(env, new LLSlotDocCodec(env, fields)));
		this.fieldValueHitQueue = ((FieldValueHitQueue) ((LMDBPriorityQueue<LLDocElement>) pq).getCodec());
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
			float score = docBase > maxMinScore.docID ? Math.nextUp(maxMinScore.score) : maxMinScore.score;
			if (score > minCompetitiveScore) {
				assert hitsThresholdChecker.isThresholdReached();
				scorer.setMinCompetitiveScore(score);
				minCompetitiveScore = score;
				totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
			}
		}
	}

	protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
		var pqTop = pq.top();
		if (hitsThresholdChecker.isThresholdReached()
				&& pqTop != null
				&& pqTop.score() != Float.NEGATIVE_INFINITY) {
			float localMinScore = Math.nextUp(pqTop.score());
			if (localMinScore > minCompetitiveScore) {
				scorer.setMinCompetitiveScore(localMinScore);
				totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
				minCompetitiveScore = localMinScore;
				if (minScoreAcc != null) {
					minScoreAcc.accumulate(pqTop.doc(), pqTop.score());
				}
			}
		}
	}
}