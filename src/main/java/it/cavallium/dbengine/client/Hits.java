package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.collections.ValueGetter;
import it.cavallium.dbengine.database.collections.ValueTransformer;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class Hits<T> extends SimpleResource implements DiscardingCloseable {

	private static final Logger LOG = LogManager.getLogger(Hits.class);
	private static final Hits<?> EMPTY_HITS = new Hits<>(Flux.empty(), TotalHitsCount.of(0, true), false);
	private final Flux<T> results;
	private final TotalHitsCount totalHitsCount;

	public Hits(Flux<T> results, TotalHitsCount totalHitsCount) {
		this(results, totalHitsCount, true);
	}

	private Hits(Flux<T> results, TotalHitsCount totalHitsCount, boolean canClose) {
		super(canClose);
		this.results = results;
		this.totalHitsCount = totalHitsCount;
	}

	@SuppressWarnings("unchecked")
	public static <T> Hits<T> empty() {
		return (Hits<T>) EMPTY_HITS;
	}

	public static <T, U> Function<Hits<HitKey<T>>, Hits<LazyHitEntry<T, U>>> generateMapper(
			ValueGetter<T, U> valueGetter) {
		return result -> {
			var hitsToTransform = result.results()
					.map(hit -> new LazyHitEntry<>(Mono.just(hit.key()), valueGetter.get(hit.key()), hit.score()));
			return Hits.withResource(hitsToTransform, result.totalHitsCount(), result);
		};
	}

	public static <T, U> Function<Hits<HitKey<T>>, Hits<LazyHitEntry<T, U>>> generateMapper(
			ValueTransformer<T, U> valueTransformer) {
		return result -> {
			try {
				var sharedHitsFlux = result.results().publish().refCount(3);
				var scoresFlux = sharedHitsFlux.map(HitKey::score);
				var keysFlux = sharedHitsFlux.map(HitKey::key);

				var valuesFlux = valueTransformer.transform(keysFlux);

				var transformedFlux = Flux.zip((Object[] data) -> {
					//noinspection unchecked
					var keyMono = Mono.just((T) data[0]);
					//noinspection unchecked
					var val = (Entry<T, Optional<U>>) data[1];
					var valMono = Mono.justOrEmpty(val.getValue());
					var score = (Float) data[2];
					return new LazyHitEntry<>(keyMono, valMono, score);
				}, keysFlux, valuesFlux, scoresFlux);

				return Hits.withResource(transformedFlux, result.totalHitsCount(), result);
			} catch (Throwable t) {
				result.close();
				throw t;
			}
		};
	}

	public static <T> Hits<T> withResource(Flux<T> hits, TotalHitsCount count, SafeCloseable resource) {
		if (resource instanceof LuceneCloseable luceneCloseable) {
			return new LuceneHits<>(hits, count, luceneCloseable);
		} else {
			return new CloseableHits<>(hits, count, resource);
		}
	}

	public Flux<T> results() {
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

		public LuceneHits(Flux<U> hits, TotalHitsCount count, LuceneCloseable resource) {
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

		public CloseableHits(Flux<U> hits, TotalHitsCount count, SafeCloseable resource) {
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
