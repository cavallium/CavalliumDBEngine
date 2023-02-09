package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.collections.ValueGetter;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Hits<T> extends SimpleResource implements DiscardingCloseable {

	private static final Logger LOG = LogManager.getLogger(Hits.class);
	private static final Hits<?> EMPTY_HITS = new Hits<>(Stream.empty(), TotalHitsCount.of(0, true), false);
	private final Stream<T> results;
	private final TotalHitsCount totalHitsCount;

	public Hits(Stream<T> results, TotalHitsCount totalHitsCount) {
		this(results, totalHitsCount, true);
	}

	private Hits(Stream<T> results, TotalHitsCount totalHitsCount, boolean canClose) {
		super(canClose);
		this.results = results;
		this.totalHitsCount = totalHitsCount;
	}

	@SuppressWarnings("unchecked")
	public static <T> Hits<T> empty() {
		return (Hits<T>) EMPTY_HITS;
	}

	public static <T, U> Function<Hits<HitKey<T>>, Hits<HitEntry<T, U>>> generateMapper(
			ValueGetter<T, U> valueGetter) {
		return result -> {
			var hitsToTransform = result.results()
					.map(hit -> new HitEntry<>(hit.key(), valueGetter.get(hit.key()), hit.score()));
			return Hits.withResource(hitsToTransform, result.totalHitsCount(), result);
		};
	}

	public static <T> Hits<T> withResource(Stream<T> hits, TotalHitsCount count, SafeCloseable resource) {
		if (resource instanceof LuceneCloseable luceneCloseable) {
			return new LuceneHits<>(hits, count, luceneCloseable);
		} else {
			return new CloseableHits<>(hits, count, resource);
		}
	}

	public Stream<T> results() {
		ensureOpen();
		return results;
	}

	public TotalHitsCount totalHitsCount() {
		ensureOpen();
		return totalHitsCount;
	}

	@Override
	public String toString() {
		return "Hits[" + "results=" + results + ", " + "totalHitsCount=" + totalHitsCount + ']';
	}

	@Override
	protected void onClose() {
	}

	public static final class LuceneHits<U> extends Hits<U> implements LuceneCloseable {

		private final LuceneCloseable resource;

		public LuceneHits(Stream<U> hits, TotalHitsCount count, LuceneCloseable resource) {
			super(hits, count);
			this.resource = resource;
		}

		@Override
		protected void onClose() {
			try {
				resource.close();
			} catch (Throwable ex) {
				LOG.error("Failed to close resource", ex);
			}
			super.onClose();
		}
	}

	public static final class CloseableHits<U> extends Hits<U> {

		private final SafeCloseable resource;

		public CloseableHits(Stream<U> hits, TotalHitsCount count, SafeCloseable resource) {
			super(hits, count);
			this.resource = resource;
		}

		@Override
		protected void onClose() {
			try {
				resource.close();
			} catch (Throwable ex) {
				LOG.error("Failed to close resource", ex);
			}
			super.onClose();
		}
	}
}
