package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.disk.LLLocalKeyValueDatabase;
import java.io.IOException;
import java.util.Objects;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class LuceneSearchResult extends ResourceSupport<LuceneSearchResult, LuceneSearchResult> {

	private static final Logger logger = LoggerFactory.getLogger(LuceneSearchResult.class);

	private TotalHitsCount totalHitsCount;
	private Flux<LLKeyScore> results;

	public LuceneSearchResult(TotalHitsCount totalHitsCount, Flux<LLKeyScore> results, Drop<LuceneSearchResult> drop) {
		super(new LuceneSearchResult.CloseOnDrop(drop));
		this.totalHitsCount = totalHitsCount;
		this.results = results;
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
		makeInaccessible();
		return drop -> new LuceneSearchResult(totalHitsCount, results, drop);
	}

	private void makeInaccessible() {
		this.totalHitsCount = null;
		this.results = null;
	}

	private static class CloseOnDrop implements Drop<LuceneSearchResult> {

		private final Drop<LuceneSearchResult> delegate;

		public CloseOnDrop(Drop<LuceneSearchResult> drop) {
			this.delegate = drop;
		}

		@Override
		public void drop(LuceneSearchResult obj) {
			delegate.drop(obj);
		}
	}

}
