package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.DatabaseResourceSupport;
import java.util.Objects;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;

public final class LuceneSearchResult extends DatabaseResourceSupport<LuceneSearchResult, LuceneSearchResult> {

	private static final Logger logger = LoggerFactory.getLogger(LuceneSearchResult.class);

	private static final Drop<LuceneSearchResult> DROP = new Drop<>() {
		@Override
		public void drop(LuceneSearchResult obj) {
			try {
				if (obj.onClose != null) {
					obj.onClose.run();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close onClose", ex);
			}
		}

		@Override
		public Drop<LuceneSearchResult> fork() {
			return this;
		}

		@Override
		public void attach(LuceneSearchResult obj) {

		}
	};

	private TotalHitsCount totalHitsCount;
	private Flux<LLKeyScore> results;
	private Runnable onClose;

	public LuceneSearchResult(TotalHitsCount totalHitsCount, Flux<LLKeyScore> results, Runnable onClose) {
		super(DROP);
		this.totalHitsCount = totalHitsCount;
		this.results = results;
		this.onClose = onClose;
	}

	public TotalHitsCount totalHitsCount() {
		if (!isOwned()) {
			throw attachTrace(new IllegalStateException("LuceneSearchResult must be owned to be used"));
		}
		return totalHitsCount;
	}

	public Flux<LLKeyScore> results() {
		if (!isOwned()) {
			throw attachTrace(new IllegalStateException("LuceneSearchResult must be owned to be used"));
		}
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
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LuceneSearchResult> prepareSend() {
		var totalHitsCount = this.totalHitsCount;
		var results = this.results;
		var onClose = this.onClose;
		return drop -> {
			var instance = new LuceneSearchResult(totalHitsCount, results, onClose);
			drop.attach(instance);
			return instance;
		};
	}

	protected void makeInaccessible() {
		this.totalHitsCount = null;
		this.results = null;
		this.onClose = null;
	}

}
