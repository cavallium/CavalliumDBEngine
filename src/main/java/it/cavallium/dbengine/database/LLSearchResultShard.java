package it.cavallium.dbengine.database;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;

public final class LLSearchResultShard extends SimpleResource implements DiscardingCloseable {

	private static final Logger logger = LogManager.getLogger(LLSearchResultShard.class);

	private final Flux<LLKeyScore> results;
	private final TotalHitsCount totalHitsCount;
	private final Runnable onClose;

	public LLSearchResultShard(Flux<LLKeyScore> results, TotalHitsCount totalHitsCount, Runnable onClose) {
		this.results = results;
		this.totalHitsCount = totalHitsCount;
		this.onClose = onClose;
	}

	public Flux<LLKeyScore> results() {
		ensureOpen();
		return results;
	}

	public TotalHitsCount totalHitsCount() {
		ensureOpen();
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

	@Override
	public void onClose() {
		try {
			var onClose = this.onClose;
			if (onClose != null) {
				onClose.run();
			}
		} catch (Throwable ex) {
			logger.error("Failed to close onClose", ex);
		}
	}
}
