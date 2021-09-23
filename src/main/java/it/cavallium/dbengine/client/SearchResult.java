package it.cavallium.dbengine.client;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.LiveResourceSupport;
import java.util.Objects;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class SearchResult<T, U> extends LiveResourceSupport<SearchResult<T, U>, SearchResult<T, U>> {

	private Flux<SearchResultItem<T, U>> results;
	private TotalHitsCount totalHitsCount;

	public SearchResult(Flux<SearchResultItem<T, U>> results, TotalHitsCount totalHitsCount,
			Drop<SearchResult<T, U>> drop) {
		super(drop);
		this.results = results;
		this.totalHitsCount = totalHitsCount;
	}

	public static <T, U> SearchResult<T, U> empty() {
		return new SearchResult<T, U>(Flux.empty(), TotalHitsCount.of(0, true), d -> {});
	}

	public Flux<SearchResultItem<T, U>> results() {
		return results;
	}

	public TotalHitsCount totalHitsCount() {
		return totalHitsCount;
	}

	@Override
	public String toString() {
		return "SearchResult[" + "results=" + results + ", " + "totalHitsCount=" + totalHitsCount + ']';
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<SearchResult<T, U>> prepareSend() {
		var results = this.results;
		var totalHitsCount = this.totalHitsCount;
		return drop -> new SearchResult<>(results, totalHitsCount, drop);
	}

	protected void makeInaccessible() {
		this.results = null;
		this.totalHitsCount = null;
	}
}
