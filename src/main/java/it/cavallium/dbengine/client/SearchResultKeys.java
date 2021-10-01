package it.cavallium.dbengine.client;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LiveResourceSupport;
import it.cavallium.dbengine.database.collections.ValueGetter;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;

@SuppressWarnings("unused")
public final class SearchResultKeys<T> extends LiveResourceSupport<SearchResultKeys<T>, SearchResultKeys<T>> {

	private static final Logger logger = LoggerFactory.getLogger(SearchResultKeys.class);

	private static final Drop<SearchResultKeys<?>> DROP = new Drop<>() {
		@Override
		public void drop(SearchResultKeys<?> obj) {
			if (obj.onClose != null) {
				obj.onClose.run();
			}
		}

		@Override
		public Drop<SearchResultKeys<?>> fork() {
			return this;
		}

		@Override
		public void attach(SearchResultKeys<?> obj) {

		}
	};

	private Flux<SearchResultKey<T>> results;
	private TotalHitsCount totalHitsCount;
	private Runnable onClose;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public SearchResultKeys(Flux<SearchResultKey<T>> results, TotalHitsCount totalHitsCount, Runnable onClose) {
		super((Drop<SearchResultKeys<T>>) (Drop) DROP);
		this.results = results;
		this.totalHitsCount = totalHitsCount;
		this.onClose = onClose;
	}

	public static <T> SearchResultKeys<T> empty() {
		return new SearchResultKeys<>(Flux.empty(), TotalHitsCount.of(0, true), null);
	}

	public <U> SearchResult<T, U> withValues(ValueGetter<T, U> valuesGetter) {
		return new SearchResult<>(results.map(item -> new SearchResultItem<>(item.key(),
				item.key().flatMap(valuesGetter::get),
				item.score()
		)), totalHitsCount, this::close);
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
		var onClose = this.onClose;
		makeInaccessible();
		return drop -> {
			var instance = new SearchResultKeys<>(results, totalHitsCount, onClose);
			drop.attach(instance);
			return instance;
		};
	}

	protected void makeInaccessible() {
		this.results = null;
		this.totalHitsCount = null;
		this.onClose = null;
	}

}
