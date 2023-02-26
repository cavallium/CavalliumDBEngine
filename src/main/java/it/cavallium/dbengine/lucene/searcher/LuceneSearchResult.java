package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LuceneSearchResult {

	public static final TotalHitsCount EMPTY_COUNT = new TotalHitsCount(0, true);

	public static final LuceneSearchResult EMPTY = new LuceneSearchResult(EMPTY_COUNT, List.of());
	private static final Logger logger = LogManager.getLogger(LuceneSearchResult.class);

	private final TotalHitsCount totalHitsCount;
	private final List<LLKeyScore> results;

	public LuceneSearchResult(TotalHitsCount totalHitsCount, List<LLKeyScore> results) {
		this.totalHitsCount = totalHitsCount;
		this.results = results;
	}

	public TotalHitsCount totalHitsCount() {
		return totalHitsCount;
	}

	public List<LLKeyScore> results() {
		return results;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;
		var that = (LuceneSearchResult) obj;
		return this.totalHitsCount == that.totalHitsCount && Objects.equals(this.results, that.results);
	}

	@Override
	public int hashCode() {
		return Objects.hash(totalHitsCount, results);
	}

	@Override
	public String toString() {
		return "LuceneSearchResult[" + "totalHitsCount=" + totalHitsCount + ", " + "results=" + results + ']';
	}

}
