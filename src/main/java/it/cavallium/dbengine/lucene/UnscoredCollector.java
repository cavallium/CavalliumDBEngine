package it.cavallium.dbengine.lucene;

import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.ALLOW_UNSCORED_PAGINATION_MODE;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;

import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.jetbrains.annotations.Nullable;

public class UnscoredCollector extends TopDocsCollector<ScoreDoc> implements LeafCollector {
	private final IntArrayList docIds = new IntArrayList();
	private final int limit;
	private final boolean doAfterDocCalculation;
	private final int afterDocId;
	private LeafReaderContext currentLeafReaderContext;

	private boolean isLastElementOrdered = true;
	private int biggestDocId = -1;
	private int biggestDocIdIndex;

	public UnscoredCollector(@Nullable Integer afterDocId, int limit) {
		super(null);
		if (!ALLOW_UNSCORED_PAGINATION_MODE) {
			throw new UnsupportedOperationException();
		}
		if (limit <= 0) {
			throw new IllegalArgumentException();
		}
		this.limit = limit;
		if (afterDocId != null) {
			this.doAfterDocCalculation = true;
			this.afterDocId = afterDocId;
		} else {
			this.doAfterDocCalculation = false;
			this.afterDocId = -1;
		}
	}

	public UnscoredCollector(@Nullable Integer afterDocId) {
		super(null);
		this.limit = -1;
		if (afterDocId != null) {
			this.doAfterDocCalculation = true;
			this.afterDocId = afterDocId;
		} else {
			this.doAfterDocCalculation = false;
			this.afterDocId = -1;
		}
	}

	@Override
	public void setScorer(Scorable scorable) {
	}

	@Override
	public void collect(int localDocId) {
		totalHits++;
		boolean canCollect;
		if (limit == -1 || docIds.size() < limit) {
			if (doAfterDocCalculation) {
				canCollect = localDocId > (this.afterDocId - currentLeafReaderContext.docBase);
			} else {
				canCollect = true;
			}
		} else {
			canCollect = false;
		}
		if (canCollect) {
			int docId = currentLeafReaderContext.docBase + localDocId;
			if (docIds.add(docId)) {
				if (docId > biggestDocId) {
					isLastElementOrdered = true;
					int docIndex = docIds.size() - 1;
					biggestDocId = docId;
					biggestDocIdIndex = docIndex;
				} else {
					isLastElementOrdered = false;
				}
			}
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
			results[i] = new ScoreDoc(docId, 1.0f);
			i++;
		}
		if (!isLastElementOrdered || start + howMany < docIds.size()) {
			int lastIndex = results.length - 1;
			var previousLastDoc = results[lastIndex];
			var biggestDoc = results[biggestDocIdIndex];
			results[lastIndex] = biggestDoc;
			results[biggestDocIdIndex] = previousLastDoc;
		}
	}

	@Override
	protected void populateResults(ScoreDoc[] results, int howMany) {
		throw new UnsupportedOperationException();
	}
}