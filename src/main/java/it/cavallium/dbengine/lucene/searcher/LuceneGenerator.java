package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LuceneGenerator implements Supplier<ScoreDoc> {

	private static final Scheduler SCHED = uninterruptibleScheduler(Schedulers.boundedElastic());
	private final IndexSearcher shard;
	private final int shardIndex;
	private final Query query;
	private final Iterator<LeafReaderContext> leavesIterator;
	private final boolean computeScores;

	private long remainingOffset;
	private long remainingAllowedResults;
	private Weight weight;

	private LeafReaderContext leaf;
	private DocIdSetIterator docIdSetIterator;
	private Scorer scorer;

	LuceneGenerator(IndexSearcher shard, LocalQueryParams localQueryParams, int shardIndex) {
		this.shard = shard;
		this.shardIndex = shardIndex;
		this.query = localQueryParams.query();
		this.remainingOffset = localQueryParams.offsetLong();
		this.remainingAllowedResults = localQueryParams.limitLong();
		this.computeScores = localQueryParams.needsScores();
		List<LeafReaderContext> leaves = shard.getTopReaderContext().leaves();
		this.leavesIterator = leaves.iterator();
	}

	public static Flux<ScoreDoc> reactive(IndexSearcher shard, LocalQueryParams localQueryParams, int shardIndex) {
		if (localQueryParams.sort() != null) {
			return Flux.error(new IllegalArgumentException("Sorting is not allowed"));
		}
		return Flux
				.<ScoreDoc, LuceneGenerator>generate(() -> new LuceneGenerator(shard, localQueryParams, shardIndex),
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
				.subscribeOn(SCHED, false);
	}

	@Override
	public ScoreDoc get() {
		while (remainingOffset > 0) {
			skipNext();
		}
		if (remainingAllowedResults == 0) {
			return null;
		} else {
			remainingAllowedResults--;
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

	private void clearState() {
		docIdSetIterator = null;
		scorer = null;
		leaf = null;
	}
}
