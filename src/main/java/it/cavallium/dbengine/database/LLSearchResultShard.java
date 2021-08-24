package it.cavallium.dbengine.database;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.lucene.searcher.LuceneSearchResult;
import java.util.Objects;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class LLSearchResultShard {

	private static final Logger logger = LoggerFactory.getLogger(LLSearchResultShard.class);

	private volatile boolean releaseCalled;

	private final Flux<LLKeyScore> results;
	private final TotalHitsCount totalHitsCount;
	private final Mono<Void> release;

	public LLSearchResultShard(Flux<LLKeyScore> results, TotalHitsCount totalHitsCount, Mono<Void> release) {
		this.results = results;
		this.totalHitsCount = totalHitsCount;
		this.release = Mono.fromRunnable(() -> {
			if (releaseCalled) {
				logger.warn("LuceneSearchResult::release has been called twice!");
			}
			releaseCalled = true;
		}).then(release);
	}

	public Flux<LLKeyScore> results() {
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
		var that = (LLSearchResultShard) obj;
		return Objects.equals(this.results, that.results) && Objects.equals(this.totalHitsCount, that.totalHitsCount)
				&& Objects.equals(this.release, that.release);
	}

	@Override
	public int hashCode() {
		return Objects.hash(results, totalHitsCount, release);
	}

	@Override
	public String toString() {
		return "LLSearchResultShard[" + "results=" + results + ", " + "totalHitsCount=" + totalHitsCount + ", " + "release="
				+ release + ']';
	}

	@SuppressWarnings("deprecation")
	@Override
	protected void finalize() throws Throwable {
		if (!releaseCalled) {
			logger.warn("LuceneSearchResult::release has not been called before class finalization!");
		}
		super.finalize();
	}

}
