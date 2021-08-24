package it.cavallium.dbengine.client;

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
public final class SearchResultKeys<T> {

	private static final Logger logger = LoggerFactory.getLogger(SearchResultKeys.class);

	private volatile boolean releaseCalled;

	private final Flux<SearchResultKey<T>> results;
	private final TotalHitsCount totalHitsCount;
	private final Mono<Void> release;

	public SearchResultKeys(Flux<SearchResultKey<T>> results, TotalHitsCount totalHitsCount, Mono<Void> release) {
		this.results = results;
		this.totalHitsCount = totalHitsCount;
		this.release = Mono.fromRunnable(() -> {
			if (releaseCalled) {
				logger.warn(this.getClass().getName() + "::release has been called twice!");
			}
			releaseCalled = true;
		}).then(release);
	}

	public static <T, U> SearchResultKeys<T> empty() {
		return new SearchResultKeys<>(Flux.empty(), TotalHitsCount.of(0, true), Mono.empty());
	}

	public <U> SearchResult<T, U> withValues(ValueGetter<T, U> valuesGetter) {
		return new SearchResult<>(results.map(item -> new SearchResultItem<>(item.key(),
				item.key().flatMap(valuesGetter::get),
				item.score()
		)), totalHitsCount, release);
	}

	public Flux<SearchResultKey<T>> resultsThenRelease() {
		return Flux.usingWhen(Mono.just(true), _unused -> results, _unused -> release);
	}

	public Flux<SearchResultKey<T>> results() {
		return results;
	}

	public TotalHitsCount totalHitsCount() {
		return totalHitsCount;
	}

	public Mono<Void> release() {
		return release;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;
		var that = (SearchResultKeys) obj;
		return Objects.equals(this.results, that.results) && Objects.equals(this.totalHitsCount, that.totalHitsCount)
				&& Objects.equals(this.release, that.release);
	}

	@Override
	public int hashCode() {
		return Objects.hash(results, totalHitsCount, release);
	}

	@Override
	public String toString() {
		return "SearchResultKeys[" + "results=" + results + ", " + "totalHitsCount=" + totalHitsCount + ", " + "release="
				+ release + ']';
	}

	@SuppressWarnings("deprecation")
	@Override
	protected void finalize() throws Throwable {
		if (!releaseCalled) {
			logger.warn(this.getClass().getName() + "::release has not been called before class finalization!");
		}
		super.finalize();
	}

}
