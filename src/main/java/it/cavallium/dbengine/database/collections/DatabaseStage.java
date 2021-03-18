package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public interface DatabaseStage<T> extends DatabaseStageWithEntry<T> {

	default Mono<T> get(@Nullable CompositeSnapshot snapshot) {
		return get(snapshot, false);
	}

	Mono<T> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly);

	default Mono<T> getOrDefault(@Nullable CompositeSnapshot snapshot,
			Mono<T> defaultValue,
			boolean existsAlmostCertainly) {
		return get(snapshot, existsAlmostCertainly).switchIfEmpty(defaultValue).single();
	}

	default Mono<T> getOrDefault(@Nullable CompositeSnapshot snapshot, Mono<T> defaultValue) {
		return getOrDefault(snapshot, defaultValue, false);
	}

	default Mono<Void> set(T value) {
		return setAndGetStatus(value).then();
	}

	Mono<T> setAndGetPrevious(T value);

	default Mono<Boolean> setAndGetStatus(T value) {
		return setAndGetPrevious(value).map(oldValue -> !Objects.equals(oldValue, value)).defaultIfEmpty(false);
	}

	Mono<Boolean> update(Function<Optional<T>, Optional<T>> updater, boolean existsAlmostCertainly);

	default Mono<Boolean> update(Function<Optional<T>, Optional<T>> updater) {
		return update(updater, false);
	}

	default Mono<Void> clear() {
		return clearAndGetStatus().then();
	}

	Mono<T> clearAndGetPrevious();

	default Mono<Boolean> clearAndGetStatus() {
		return clearAndGetPrevious().map(Objects::nonNull).defaultIfEmpty(false);
	}

	default Mono<Void> close() {
		return Mono.empty();
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
}
