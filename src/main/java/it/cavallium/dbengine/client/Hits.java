package it.cavallium.dbengine.client;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.collections.ValueGetter;
import it.cavallium.dbengine.database.collections.ValueTransformer;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

public final class Hits<T> extends SimpleResource implements DiscardingCloseable {

	private static final Hits<?> EMPTY_HITS = new Hits<>(Flux.empty(), TotalHitsCount.of(0, true), null, false);
	private Flux<T> results;
	private TotalHitsCount totalHitsCount;
	private Runnable onClose;

	public Hits(Flux<T> results, TotalHitsCount totalHitsCount, Runnable onClose) {
		this(results, totalHitsCount, onClose, true);
	}

	private Hits(Flux<T> results, TotalHitsCount totalHitsCount, Runnable onClose, boolean canClose) {
		super(canClose && onClose != null);
		this.results = results;
		this.totalHitsCount = totalHitsCount;
		this.onClose = onClose;
	}

	@SuppressWarnings("unchecked")
	public static <T> Hits<T> empty() {
		return (Hits<T>) EMPTY_HITS;
	}

	public static <K, V> Hits<LazyHitEntry<K, V>> withValuesLazy(Hits<LazyHitKey<K>> hits,
			ValueGetter<K, V> valuesGetter) {
		var hitsEntry = hits.results().map(hitKey -> hitKey.withValue(valuesGetter::get));

		return new Hits<>(hitsEntry, hits.totalHitsCount, hits::close);
	}

	public static <K, V> Hits<HitEntry<K, V>> withValues(Hits<HitKey<K>> hits, ValueGetter<K, V> valuesGetter) {
		var hitsEntry = hits.results().flatMap(hitKey -> hitKey.withValue(valuesGetter::get));

		return new Hits<>(hitsEntry, hits.totalHitsCount, hits::close);
	}

	public static <T, U> Function<Hits<HitKey<T>>, Hits<LazyHitEntry<T, U>>> generateMapper(
			ValueGetter<T, U> valueGetter) {
		return result -> {
			var hitsToTransform = result.results()
					.map(hit -> new LazyHitEntry<>(Mono.just(hit.key()), valueGetter.get(hit.key()), hit.score()));
			return new Hits<>(hitsToTransform, result.totalHitsCount(), result::close);
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

				return new Hits<>(transformedFlux, result.totalHitsCount(), result::close);
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
		if (onClose != null) {
			onClose.run();
		}
	}
}
