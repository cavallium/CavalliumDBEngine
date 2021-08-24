package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLSearchResultShard;
import java.util.Objects;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class SearchResult<T, U> {

	private static final Logger logger = LoggerFactory.getLogger(SearchResult.class);

	private volatile boolean releaseCalled;

	private final Flux<SearchResultItem<T, U>> results;
	private final TotalHitsCount totalHitsCount;
	private final Mono<Void> release;

	public SearchResult(Flux<SearchResultItem<T, U>> results, TotalHitsCount totalHitsCount, Mono<Void> release) {
		this.results = results;
		this.totalHitsCount = totalHitsCount;
		this.release = Mono.fromRunnable(() -> {
			if (releaseCalled) {
				logger.warn(this.getClass().getName() + "::release has been called twice!");
			}
			releaseCalled = true;
		}).then(release);
	}

	public static <T, U> SearchResult<T, U> empty() {
		var sr = new SearchResult<T, U>(Flux.empty(), TotalHitsCount.of(0, true), Mono.empty());
		sr.releaseCalled = true;
		return sr;
	}

	public Flux<SearchResultItem<T, U>> resultsThenRelease() {
		return Flux.usingWhen(Mono.just(true), _unused -> results, _unused -> release);
	}

	public Flux<SearchResultItem<T, U>> results() {
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
		var that = (SearchResult) obj;
		return Objects.equals(this.results, that.results) && Objects.equals(this.totalHitsCount, that.totalHitsCount)
				&& Objects.equals(this.release, that.release);
	}

	@Override
	public int hashCode() {
		return Objects.hash(results, totalHitsCount, release);
	}

	@Override
	public String toString() {
		return "SearchResult[" + "results=" + results + ", " + "totalHitsCount=" + totalHitsCount + ", " + "release="
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
