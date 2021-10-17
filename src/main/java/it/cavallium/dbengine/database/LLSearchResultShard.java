package it.cavallium.dbengine.database;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import java.util.Objects;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;

public final class LLSearchResultShard extends ResourceSupport<LLSearchResultShard, LLSearchResultShard> {

	private static final Logger logger = LoggerFactory.getLogger(LLSearchResultShard.class);

	private static final Drop<LLSearchResultShard> DROP = new Drop<>() {
		@Override
		public void drop(LLSearchResultShard obj) {
			try {
				if (obj.onClose != null) {
					obj.onClose.run();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close onClose", ex);
			}
		}

		@Override
		public Drop<LLSearchResultShard> fork() {
			return this;
		}

		@Override
		public void attach(LLSearchResultShard obj) {

		}
	};

	private Flux<LLKeyScore> results;
	private TotalHitsCount totalHitsCount;
	private Runnable onClose;

	public LLSearchResultShard(Flux<LLKeyScore> results, TotalHitsCount totalHitsCount, Runnable onClose) {
		super(DROP);
		this.results = results;
		this.totalHitsCount = totalHitsCount;
		this.onClose = onClose;
	}

	public Flux<LLKeyScore> results() {
		if (!isOwned()) {
			throw attachTrace(new IllegalStateException("LLSearchResultShard must be owned to be used"));
		}
		return results;
	}

	public TotalHitsCount totalHitsCount() {
		if (!isOwned()) {
			throw attachTrace(new IllegalStateException("LLSearchResultShard must be owned to be used"));
		}
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
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLSearchResultShard> prepareSend() {
		var results = this.results;
		var totalHitsCount = this.totalHitsCount;
		var onClose = this.onClose;
		return drop -> new LLSearchResultShard(results, totalHitsCount, onClose);
	}

	protected void makeInaccessible() {
		this.results = null;
		this.totalHitsCount = null;
		this.onClose = null;
	}
}
