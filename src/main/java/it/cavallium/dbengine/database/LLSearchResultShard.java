package it.cavallium.dbengine.database;

import io.netty5.buffer.Drop;
import io.netty5.buffer.Owned;
import io.netty5.buffer.internal.ResourceSupport;
import it.cavallium.dbengine.client.LuceneIndexImpl;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.utils.SimpleResource;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;

public class LLSearchResultShard extends SimpleResource implements DiscardingCloseable {

	private static final Logger LOG = LogManager.getLogger(LLSearchResultShard.class);

	private final Flux<LLKeyScore> results;
	private final TotalHitsCount totalHitsCount;

	public LLSearchResultShard(Flux<LLKeyScore> results, TotalHitsCount totalHitsCount) {
		this.results = results;
		this.totalHitsCount = totalHitsCount;
	}

	public static LLSearchResultShard withResource(Flux<LLKeyScore> results,
			TotalHitsCount totalHitsCount,
			SafeCloseable closeableResource) {
		if (closeableResource instanceof LuceneCloseable luceneCloseable) {
			return new LuceneLLSearchResultShard(results, totalHitsCount, List.of(luceneCloseable));
		} else {
			return new ResourcesLLSearchResultShard(results, totalHitsCount, List.of(closeableResource));
		}
	}

	public Flux<LLKeyScore> results() {
		ensureOpen();
		return results;
	}

	public TotalHitsCount totalHitsCount() {
		ensureOpen();
		return totalHitsCount;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;
		var that = (LLSearchResultShard) obj;
		return Objects.equals(this.results, that.results) && Objects.equals(this.totalHitsCount, that.totalHitsCount);
	}

	@Override
	public int hashCode() {
		return Objects.hash(results, totalHitsCount);
	}

	@Override
	public String toString() {
		return "LLSearchResultShard[" + "results=" + results + ", " + "totalHitsCount=" + totalHitsCount + ']';
	}

	@Override
	public void onClose() {
	}

	public static class ResourcesLLSearchResultShard extends LLSearchResultShard {

		private final List<SafeCloseable> resources;

		public ResourcesLLSearchResultShard(Flux<LLKeyScore> resultsFlux,
				TotalHitsCount count,
				List<SafeCloseable> resources) {
			super(resultsFlux, count);
			this.resources = resources;
		}

		@Override
		public void onClose() {
			try {
				for (SafeCloseable resource : resources) {
					try {
						resource.close();
					} catch (Throwable ex) {
						LOG.error("Failed to close resource", ex);
					}
				}
			} catch (Throwable ex) {
				LOG.error("Failed to close resources", ex);
			}
			super.onClose();
		}
	}

	public static class LuceneLLSearchResultShard extends LLSearchResultShard implements LuceneCloseable {

		private final List<LuceneCloseable> resources;

		public LuceneLLSearchResultShard(Flux<LLKeyScore> resultsFlux,
				TotalHitsCount count,
				List<LuceneCloseable> resources) {
			super(resultsFlux, count);
			this.resources = resources;
		}

		@Override
		public void onClose() {
			try {
				for (LuceneCloseable resource : resources) {
					try {
						resource.close();
					} catch (Throwable ex) {
						LOG.error("Failed to close resource", ex);
					}
				}
			} catch (Throwable ex) {
				LOG.error("Failed to close resources", ex);
			}
			super.onClose();
		}
	}
}
