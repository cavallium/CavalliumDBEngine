package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import java.util.concurrent.atomic.AtomicLong;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class CountLuceneMultiSearcher implements LuceneMultiSearcher {

	@Override
	public Mono<Send<LuceneMultiSearcher>> createShardSearcher(LocalQueryParams queryParams) {
		return Mono.fromCallable(() -> new CountLuceneShardSearcher(new AtomicLong(0), d -> {}).send());
	}

	private static class CountLuceneShardSearcher extends
			ResourceSupport<LuceneMultiSearcher, CountLuceneShardSearcher> implements LuceneMultiSearcher {

		private AtomicLong totalHitsCount;

		public CountLuceneShardSearcher(AtomicLong totalHitsCount, Drop<CountLuceneShardSearcher> drop) {
			super(new CloseOnDrop(drop));
			this.totalHitsCount = totalHitsCount;
		}

		@Override
		public Mono<Void> searchOn(Send<LLIndexSearcher> indexSearcher, LocalQueryParams queryParams) {
			return Mono
					.fromCallable(() -> {
						try (var is = indexSearcher.receive()) {
							if (!isOwned()) {
								throw attachTrace(new IllegalStateException("CountLuceneMultiSearcher must be owned to be used"));
							}
							LLUtils.ensureBlocking();
							totalHitsCount.addAndGet(is.getIndexSearcher().count(queryParams.query()));
							return null;
						}
					});
		}

		@Override
		public Mono<Send<LuceneSearchResult>> collect(LocalQueryParams queryParams, String keyFieldName) {
			return Mono.fromCallable(() -> {
				if (!isOwned()) {
					throw attachTrace(new IllegalStateException("CountLuceneMultiSearcher must be owned to be used"));
				}
				LLUtils.ensureBlocking();
				return new LuceneSearchResult(TotalHitsCount.of(totalHitsCount.get(), true), Flux.empty(), d -> {})
						.send();
			});
		}

		@Override
		protected RuntimeException createResourceClosedException() {
			return new IllegalStateException("Closed");
		}

		@Override
		protected Owned<CountLuceneShardSearcher> prepareSend() {
			var totalHitsCount = this.totalHitsCount;
			makeInaccessible();
			return drop -> new CountLuceneShardSearcher(totalHitsCount, drop);
		}

		private void makeInaccessible() {
			this.totalHitsCount = null;
		}

		private static class CloseOnDrop implements Drop<CountLuceneShardSearcher> {

			private final Drop<CountLuceneShardSearcher> delegate;

			public CloseOnDrop(Drop<CountLuceneShardSearcher> drop) {
				this.delegate = drop;
			}

			@Override
			public void drop(CountLuceneShardSearcher obj) {
				delegate.drop(obj);
			}
		}
	}
}
