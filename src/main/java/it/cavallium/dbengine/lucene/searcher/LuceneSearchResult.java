package it.cavallium.dbengine.lucene.searcher;

import io.netty5.buffer.Drop;
import io.netty5.buffer.Owned;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.LLKeyScore;
import io.netty5.buffer.internal.ResourceSupport;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;

public class LuceneSearchResult extends SimpleResource implements DiscardingCloseable {

	private static final Logger logger = LogManager.getLogger(LuceneSearchResult.class);

	private final TotalHitsCount totalHitsCount;
	private final Flux<LLKeyScore> results;

	public LuceneSearchResult(TotalHitsCount totalHitsCount, Flux<LLKeyScore> results) {
		this.totalHitsCount = totalHitsCount;
		this.results = results;
	}

	public TotalHitsCount totalHitsCount() {
		ensureOpen();
		return totalHitsCount;
	}

	public Flux<LLKeyScore> results() {
		ensureOpen();
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

	@Override
	protected void onClose() {
	}
}
