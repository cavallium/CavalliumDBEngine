package it.cavallium.dbengine.client;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.DatabaseResourceSupport;
import reactor.core.publisher.Flux;

public final class SearchResult<T, U> extends DatabaseResourceSupport<SearchResult<T, U>, SearchResult<T, U>> {

	private static final Drop<SearchResult<?, ?>> DROP = new Drop<>() {
		@Override
		public void drop(SearchResult<?, ?> obj) {
			if (obj.onClose != null) {
				obj.onClose.run();
			}
		}

		@Override
		public Drop<SearchResult<?, ?>> fork() {
			return this;
		}

		@Override
		public void attach(SearchResult<?, ?> obj) {

		}
	};

	private Flux<SearchResultItem<T, U>> results;
	private TotalHitsCount totalHitsCount;
	private Runnable onClose;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public SearchResult(Flux<SearchResultItem<T, U>> results, TotalHitsCount totalHitsCount, Runnable onClose) {
		super((Drop<SearchResult<T,U>>) (Drop) DROP);
		this.results = results;
		this.totalHitsCount = totalHitsCount;
		this.onClose = onClose;
	}

	public static <T, U> SearchResult<T, U> empty() {
		return new SearchResult<T, U>(Flux.empty(), TotalHitsCount.of(0, true), null);
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
		var onClose = this.onClose;
		return drop -> {
			var instance = new SearchResult<>(results, totalHitsCount, onClose);
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
