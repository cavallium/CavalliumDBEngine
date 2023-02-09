package it.cavallium.dbengine.lucene.collector;

import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.ALLOW_UNSCORED_PAGINATION_MODE;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;

public class UnscoredCollector extends TopDocsCollector<ScoreDoc> implements LeafCollector {
	private final IntArrayList docIds = new IntArrayList();
	private final int limit;
	private LeafReaderContext currentLeafReaderContext;

	public UnscoredCollector(int limit) {
		super(null);
		if (!ALLOW_UNSCORED_PAGINATION_MODE) {
			throw new UnsupportedOperationException();
		}
		if (limit <= 0) {
			throw new IllegalArgumentException();
		}
		this.limit = limit;
	}

	@Override
	public void setScorer(Scorable scorable) {
	}

	@Override
	public void collect(int localDocId) {
		totalHits++;
		boolean canCollect = limit == -1 || docIds.size() < limit;
		if (canCollect) {
			int docId = currentLeafReaderContext.docBase + localDocId;
			docIds.add(docId);
		}
	}

	@Override
	public LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) {
		this.currentLeafReaderContext = leafReaderContext;
		return this;
	}

	public IntList unscoredDocs() {
		return IntLists.unmodifiable(this.docIds);
	}

	@Override
	public ScoreMode scoreMode() {
		return ScoreMode.COMPLETE_NO_SCORES;
	}

	@Override
	protected int topDocsSize() {
		return Math.min(this.totalHits, this.docIds.size());
	}

	@Override
	public TopDocs topDocs(int start, int howMany) {
		int size = this.topDocsSize();
		if (howMany < 0) {
			throw new IllegalArgumentException("Number of hits requested must be greater than 0 but value was " + howMany);
		} else if (start < 0) {
			throw new IllegalArgumentException("Expected value of starting position is between 0 and " + size + ", got " + start);
		} else if (start < size && howMany != 0) {
			howMany = Math.min(size - start, howMany);
			ScoreDoc[] results = new ScoreDoc[howMany];

			this.populateResults(results, start, howMany);
			return this.newTopDocs(results, start);
		} else {
			return this.newTopDocs((ScoreDoc[])null, start);
		}
	}

	@Override
	protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
		return super.newTopDocs(results, start);
	}

	private void populateResults(ScoreDoc[] results, int start, int howMany) {
		int i = 0;
		for (int docId : docIds.subList(start, start + howMany)) {
			results[i] = new ScoreDoc(docId, Float.NaN);
			i++;
		}
	}

	@Override
	protected void populateResults(ScoreDoc[] results, int howMany) {
		throw new UnsupportedOperationException();
	}
}