package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import it.cavallium.dbengine.lucene.MaxScoreAccumulator;
import it.cavallium.dbengine.lucene.MaxScoreAccumulator.DocAndScore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CustomHitsThresholdChecker;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class LuceneGenerator implements Supplier<ScoreDoc> {

	private final IndexSearcher shard;
	private final int shardIndex;
	private final Query query;
	private final Iterator<LeafReaderContext> leavesIterator;
	private final boolean computeScores;
	private final CustomHitsThresholdChecker hitsThresholdChecker;
	private final MaxScoreAccumulator minScoreAcc;
	private final @Nullable Long limit;

	private long remainingOffset;
	private Weight weight;
	private Float minCompetitiveScore;
	private long totalHits;

	private LeafReaderContext leaf;
	private DocIdSetIterator docIdSetIterator;
	private Scorer scorer;

	LuceneGenerator(IndexSearcher shard,
			LocalQueryParams localQueryParams,
			int shardIndex,
			CustomHitsThresholdChecker hitsThresholdChecker,
			MaxScoreAccumulator minScoreAcc) {
		this.shard = shard;
		this.shardIndex = shardIndex;
		this.query = localQueryParams.query();
		this.remainingOffset = localQueryParams.offsetLong();
		this.limit = localQueryParams.limitLong() == Long.MAX_VALUE ? null : localQueryParams.limitLong();
		this.computeScores = localQueryParams.needsScores();
		List<LeafReaderContext> leaves = shard.getTopReaderContext().leaves();
		this.leavesIterator = leaves.iterator();
		this.hitsThresholdChecker = hitsThresholdChecker;
		this.minScoreAcc = minScoreAcc;
	}

	public static Flux<ScoreDoc> reactive(IndexSearcher shard,
			LocalQueryParams localQueryParams,
			int shardIndex,
			CustomHitsThresholdChecker hitsThresholdChecker,
			MaxScoreAccumulator minScoreAcc) {
		return Flux
				.<ScoreDoc, LuceneGenerator>generate(() -> new LuceneGenerator(shard,
								localQueryParams,
								shardIndex,
								hitsThresholdChecker,
								minScoreAcc
						),
						(s, sink) -> {
							ScoreDoc val = s.get();
							if (val == null) {
								sink.complete();
							} else {
								sink.next(val);
							}
							return s;
						}
				)
				.subscribeOn(uninterruptibleScheduler(Schedulers.boundedElastic()), false);
	}

	@Override
	public ScoreDoc get() {
		while (remainingOffset > 0) {
			skipNext();
		}
		if ((limit != null && totalHits > limit) || hitsThresholdChecker.isThresholdReached(true)) {
			return null;
		}
		return getNext();
	}

	public void skipNext() {
		getNext();
		remainingOffset--;
	}

	private Weight createWeight() throws IOException {
		ScoreMode scoreMode = computeScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
		return shard.createWeight(shard.rewrite(query), scoreMode, 1f);
	}

	public ScoreDoc getNext() {
		if (weight == null) {
			try {
				weight = createWeight();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		try {
			return getWeightedNext();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private ScoreDoc getWeightedNext() throws IOException {
		while (tryAdvanceDocIdSetIterator()) {
			LeafReader reader = leaf.reader();
			Bits liveDocs = reader.getLiveDocs();
			int doc;
			while ((doc = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
				if (docDeleted(liveDocs, doc)) {
					continue;
				}

				float score = scorer.score();

				// This collector relies on the fact that scorers produce positive values:
				assert score >= 0; // NOTE: false for NaN

				totalHits++;
				hitsThresholdChecker.incrementHitCount();

				if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
					updateGlobalMinCompetitiveScore(scorer);
				}

				return transformDoc(doc);
			}
			docIdSetIterator = null;
		}
		clearState();
		return null;
	}
	private boolean tryAdvanceDocIdSetIterator() throws IOException {
		if (docIdSetIterator != null) {
			return true;
		}
		while (leavesIterator.hasNext()) {
			LeafReaderContext leaf = leavesIterator.next();
			Scorer scorer = weight.scorer(leaf);
			if (scorer == null) {
				continue;
			}
			this.scorer = scorer;
			this.leaf = leaf;
			this.docIdSetIterator = scorer.iterator();
			if (minScoreAcc != null) {
				updateGlobalMinCompetitiveScore(scorer);
			}
			return true;
		}
		return false;
	}

	private ScoreDoc transformDoc(int doc) throws IOException {
		return new ScoreDoc(leaf.docBase + doc, scorer.score(), shardIndex);
	}

	private static boolean docDeleted(@Nullable Bits liveDocs, int doc) {
		if (liveDocs == null) {
			return false;
		}
		return !liveDocs.get(doc);
	}

	protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
		assert minScoreAcc != null;
		DocAndScore maxMinScore = minScoreAcc.get();
		if (maxMinScore != null) {
			// since we tie-break on doc id and collect in doc id order we can require
			// the next float if the global minimum score is set on a document id that is
			// smaller than the ids in the current leaf
			float score =
					leaf.docBase >= maxMinScore.docBase ? Math.nextUp(maxMinScore.score) : maxMinScore.score;
			if (score > minCompetitiveScore) {
				assert hitsThresholdChecker.isThresholdReached(true);
				scorer.setMinCompetitiveScore(score);
				minCompetitiveScore = score;
			}
		}
	}

	private void clearState() {
		docIdSetIterator = null;
		scorer = null;
		leaf = null;
	}
}
