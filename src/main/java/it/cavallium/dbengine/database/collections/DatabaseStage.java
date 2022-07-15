package it.cavallium.dbengine.database.collections;

import io.netty5.util.Resource;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface DatabaseStage<T> extends DatabaseStageWithEntry<T>, Resource<DatabaseStage<T>> {

	Mono<T> get(@Nullable CompositeSnapshot snapshot);

	default Mono<T> getOrDefault(@Nullable CompositeSnapshot snapshot,
			Mono<T> defaultValue,
			boolean existsAlmostCertainly) {
		return get(snapshot).switchIfEmpty(defaultValue).single();
	}

	default Mono<T> getOrDefault(@Nullable CompositeSnapshot snapshot, Mono<T> defaultValue) {
		return getOrDefault(snapshot, defaultValue, false);
	}

	default Mono<Void> set(T value) {
		return this
				.setAndGetChanged(value)
				.then();
	}

	Mono<T> setAndGetPrevious(T value);

	default Mono<Boolean> setAndGetChanged(T value) {
		return this
				.setAndGetPrevious(value)
				.map(oldValue -> !Objects.equals(oldValue, value))
				.switchIfEmpty(Mono.fromSupplier(() -> value != null));
	}

	default Mono<T> update(SerializationFunction<@Nullable T, @Nullable T> updater,
			UpdateReturnMode updateReturnMode) {
		return this
				.updateAndGetDelta(updater)
				.transform(prev -> LLUtils.resolveDelta(prev, updateReturnMode));
	}

	Mono<Delta<T>> updateAndGetDelta(SerializationFunction<@Nullable T, @Nullable T> updater);

	default Mono<Void> clear() {
		return clearAndGetStatus().then();
	}

	Mono<T> clearAndGetPrevious();

	default Mono<Boolean> clearAndGetStatus() {
		return clearAndGetPrevious().map(Objects::nonNull).defaultIfEmpty(false);
	}

	/**
	 * Count all the elements.
	 * If it's a nested collection the count will include all the children recursively
	 * @param fast true to return an approximate value
	 */
	Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast);

	default Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return leavesCount(snapshot, false).map(size -> size <= 0);
	}

	Flux<BadBlock> badBlocks();
}
