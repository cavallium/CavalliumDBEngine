package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.VerificationProgress;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.util.Objects;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

public interface DatabaseStage<T> extends DatabaseStageWithEntry<T> {

	@Nullable T get(@Nullable CompositeSnapshot snapshot);

	default T getOrDefault(@Nullable CompositeSnapshot snapshot, T defaultValue, boolean existsAlmostCertainly) {
		return Objects.requireNonNullElse(get(snapshot), defaultValue);
	}

	default T getOrDefault(@Nullable CompositeSnapshot snapshot, T defaultValue) {
		return getOrDefault(snapshot, defaultValue, false);
	}

	default void set(@Nullable T value) {
		this.setAndGetChanged(value);
	}

	@Nullable T setAndGetPrevious(@Nullable T value);

	default boolean setAndGetChanged(@Nullable T value) {
		T oldValue = this.setAndGetPrevious(value);
		if (oldValue != null) {
			return !Objects.equals(oldValue, value);
		} else {
			return value != null;
		}
	}

	default @Nullable T update(SerializationFunction<@Nullable T, @Nullable T> updater, UpdateReturnMode updateReturnMode) {
		return LLUtils.resolveDelta(this.updateAndGetDelta(updater), updateReturnMode);
	}

	Delta<T> updateAndGetDelta(SerializationFunction<@Nullable T, @Nullable T> updater);

	default void clear() {
		clearAndGetStatus();
	}

	@Nullable T clearAndGetPrevious();

	default boolean clearAndGetStatus() {
		return clearAndGetPrevious() != null;
	}

	/**
	 * Count all the elements.
	 * If it's a nested collection the count will include all the children recursively
	 * @param fast true to return an approximate value
	 */
	long leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast);

	default boolean isEmpty(@Nullable CompositeSnapshot snapshot) {
		return leavesCount(snapshot, false) <= 0;
	}

	Stream<VerificationProgress> verifyChecksum();
}
