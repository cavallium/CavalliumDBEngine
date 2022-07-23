package it.cavallium.dbengine.client;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.collections.ValueGetter;
import it.cavallium.dbengine.database.collections.ValueTransformer;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

public class Hits<T> extends SimpleResource implements DiscardingCloseable {

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
			return new MappedHits<>(hitsToTransform, result.totalHitsCount(), result);
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

				return new MappedHits<>(transformedFlux, result.totalHitsCount(), result);
			} catch (Throwable t) {
				result.close();
				throw t;
			}
		};
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

	private static sealed class MappedHits<U> extends Hits<U> {

		private final Hits<?> parent;

		public MappedHits(Flux<U> hits,
				TotalHitsCount count,
				Hits<?> parent) {
			super(hits, count);
			this.parent = parent;
		}

		@Override
		protected void onClose() {
			parent.close();
			super.onClose();
		}
	}

	private static final class MappedLuceneHits<U> extends MappedHits<U> implements LuceneCloseable {

		public MappedLuceneHits(Flux<U> hits, TotalHitsCount count, Hits<?> parent) {
			super(hits, count, parent);
		}
	}
}
