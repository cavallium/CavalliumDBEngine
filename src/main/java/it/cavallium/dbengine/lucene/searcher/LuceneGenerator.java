package it.cavallium.dbengine.lucene.searcher;

import java.io.IOException;
import it.cavallium.dbengine.utils.DBException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
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

public class LuceneGenerator implements Supplier<ScoreDoc> {

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

	public static Stream<ScoreDoc> reactive(IndexSearcher shard, LocalQueryParams localQueryParams, int shardIndex) {
		if (localQueryParams.sort() != null) {
			throw new IllegalArgumentException("Sorting is not allowed");
		}
		var lg = new LuceneGenerator(shard, localQueryParams, shardIndex);
		return Stream.generate(lg).takeWhile(Objects::nonNull);
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
				throw new DBException(e);
			}
		}

		try {
			return getWeightedNext();
		} catch (IOException e) {
			throw new DBException(e);
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
