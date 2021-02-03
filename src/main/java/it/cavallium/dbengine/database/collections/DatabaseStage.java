package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public interface DatabaseStage<T> extends DatabaseStageWithEntry<T> {

	Mono<T> get(@Nullable CompositeSnapshot snapshot);

	default Mono<T> getOrDefault(@Nullable CompositeSnapshot snapshot, Mono<T> defaultValue) {
		return get(snapshot).switchIfEmpty(defaultValue).single();
	}

	default Mono<Void> set(T value) {
		return setAndGetStatus(value).then();
	}

	Mono<T> setAndGetPrevious(T value);

	default Mono<Boolean> setAndGetStatus(T value) {
		return setAndGetPrevious(value).map(oldValue -> !Objects.equals(oldValue, value)).defaultIfEmpty(false);
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

	Mono<Long> size(@Nullable CompositeSnapshot snapshot, boolean fast);

	default Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return size(snapshot, false).map(size -> size <= 0);
	}
}
