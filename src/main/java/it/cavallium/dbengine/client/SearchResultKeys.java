package it.cavallium.dbengine.client;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.collections.ValueGetter;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public final class SearchResultKeys<T> extends ResourceSupport<SearchResultKeys<T>, SearchResultKeys<T>> {

	private static final Logger logger = LoggerFactory.getLogger(SearchResultKeys.class);

	private Flux<SearchResultKey<T>> results;
	private TotalHitsCount totalHitsCount;

	public SearchResultKeys(Flux<SearchResultKey<T>> results, TotalHitsCount totalHitsCount,
			Drop<SearchResultKeys<T>> drop) {
		super(drop);
		this.results = results;
		this.totalHitsCount = totalHitsCount;
	}

	public static <T> SearchResultKeys<T> empty() {
		return new SearchResultKeys<T>(Flux.empty(), TotalHitsCount.of(0, true), d -> {});
	}

	public <U> SearchResult<T, U> withValues(ValueGetter<T, U> valuesGetter) {
		return new SearchResult<>(results.map(item -> new SearchResultItem<>(item.key(),
				item.key().flatMap(valuesGetter::get),
				item.score()
		)), totalHitsCount, d -> this.close());
	}

	public Flux<SearchResultKey<T>> results() {
		return results;
	}

	public TotalHitsCount totalHitsCount() {
		return totalHitsCount;
	}

	@Override
	public String toString() {
		return "SearchResultKeys[" + "results=" + results + ", " + "totalHitsCount=" + totalHitsCount + ']';
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<SearchResultKeys<T>> prepareSend() {
		var results = this.results;
		var totalHitsCount = this.totalHitsCount;
		makeInaccessible();
		return drop -> new SearchResultKeys<>(results, totalHitsCount, drop);
	}

	protected void makeInaccessible() {
		this.results = null;
		this.totalHitsCount = null;
	}

}
