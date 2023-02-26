package it.cavallium.dbengine.database;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LLSearchResultShard {

	private static final Logger LOG = LogManager.getLogger(LLSearchResultShard.class);

	private final List<LLKeyScore> results;
	private final TotalHitsCount totalHitsCount;

	public LLSearchResultShard(List<LLKeyScore> results, TotalHitsCount totalHitsCount) {
		this.results = results;
		this.totalHitsCount = totalHitsCount;
	}

	public List<LLKeyScore> results() {
		return results;
	}

	public TotalHitsCount totalHitsCount() {
		return totalHitsCount;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;
		var that = (LLSearchResultShard) obj;
		return Objects.equals(this.results, that.results) && Objects.equals(this.totalHitsCount, that.totalHitsCount);
	}

	@Override
	public int hashCode() {
		return Objects.hash(results, totalHitsCount);
	}

	@Override
	public String toString() {
		return "LLSearchResultShard[" + "results=" + results + ", " + "totalHitsCount=" + totalHitsCount + ']';
	}
}
