package it.cavallium.dbengine.database.collections;

import static it.cavallium.dbengine.database.LLUtils.consume;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SubStageEntry;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMaps;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public interface DatabaseStageMap<T, U, US extends DatabaseStage<U>> extends DatabaseStageEntry<Object2ObjectSortedMap<T, U>> {

	@NotNull US at(@Nullable CompositeSnapshot snapshot, T key);

	default boolean containsKey(@Nullable CompositeSnapshot snapshot, T key) {
		return !this.at(snapshot, key).isEmpty(snapshot);
	}

	default @Nullable U getValue(@Nullable CompositeSnapshot snapshot, T key) {
		return this.at(snapshot, key).get(snapshot);
	}

	default U getValueOrDefault(@Nullable CompositeSnapshot snapshot, T key, U defaultValue) {
		return Objects.requireNonNullElse(getValue(snapshot, key), defaultValue);
	}

	default U getValueOrDefault(@Nullable CompositeSnapshot snapshot, T key, Supplier<U> defaultValue) {
		return Objects.requireNonNullElseGet(getValue(snapshot, key), defaultValue);
	}

	default void putValue(T key, U value) {
		at(null, key).set(value);
	}

	UpdateMode getUpdateMode();

	default U updateValue(T key,
			UpdateReturnMode updateReturnMode,
			SerializationFunction<@Nullable U, @Nullable U> updater) {
		return at(null, key).update(updater, updateReturnMode);
	}

	default Stream<Boolean> updateMulti(Stream<T> keys, KVSerializationFunction<T, @Nullable U, @Nullable U> updater) {
		return keys.parallel().map(key -> this.updateValue(key, prevValue -> updater.apply(key, prevValue)));
	}

	default boolean updateValue(T key, SerializationFunction<@Nullable U, @Nullable U> updater) {
		return LLUtils.isDeltaChanged(updateValueAndGetDelta(key, updater));
	}

	default Delta<U> updateValueAndGetDelta(T key, SerializationFunction<@Nullable U, @Nullable U> updater) {
		return this.at(null, key).updateAndGetDelta(updater);
	}

	default @Nullable U putValueAndGetPrevious(T key, @Nullable U value) {
		return at(null, key).setAndGetPrevious(value);
	}

	/**
	 * @return true if the key was associated with any value, false if the key didn't exist.
	 */
	default boolean putValueAndGetChanged(T key, @Nullable U value) {
		return at(null, key).setAndGetChanged(value);
	}

	default void remove(T key) {
		removeAndGetStatus(key);
	}

	default @Nullable U removeAndGetPrevious(T key) {
		return at(null, key).clearAndGetPrevious();
	}

	default boolean removeAndGetStatus(T key) {
		return removeAndGetPrevious(key) != null;
	}

	/**
	 * GetMulti must return the elements in sequence!
	 */
	default Stream<Optional<U>> getMulti(@Nullable CompositeSnapshot snapshot, Stream<T> keys) {
		return keys.parallel().map(key -> Optional.ofNullable(this.getValue(snapshot, key)));
	}

	default void putMulti(Stream<Entry<T, U>> entries) {
		try (var stream = entries.parallel()) {
			stream.forEach(entry -> this.putValue(entry.getKey(), entry.getValue()));
		}
	}

	Stream<SubStageEntry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot, boolean smallRange);

	default Stream<Entry<T, U>> getAllValues(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return this.getAllStages(snapshot, smallRange).parallel().mapMulti((stage, mapper) -> {
			var val = stage.getValue().get(snapshot);
			if (val != null) {
				mapper.accept(Map.entry(stage.getKey(), val));
			}
		});
	}

	default void setAllValues(Stream<Entry<T, U>> entries) {
		consume(setAllValuesAndGetPrevious(entries));
	}

	Stream<Entry<T, U>> setAllValuesAndGetPrevious(Stream<Entry<T, U>> entries);

	default void clear() {
		setAllValues(Stream.empty());
	}

	default void replaceAllValues(boolean canKeysChange,
			Function<Entry<T, U>, @NotNull Entry<T, U>> entriesReplacer,
			boolean smallRange) {
		if (canKeysChange) {
			this.setAllValues(this.getAllValues(null, smallRange).map(entriesReplacer));
		} else {
			this.getAllValues(null, smallRange).map(entriesReplacer)
					.forEach(replacedEntry -> this.at(null, replacedEntry.getKey()).set(replacedEntry.getValue()));
		}
	}

	default void replaceAll(Consumer<Entry<T, US>> entriesReplacer) {
		this.getAllStages(null, false).forEach(entriesReplacer);
	}

	@Override
	default Object2ObjectSortedMap<T, U> setAndGetPrevious(Object2ObjectSortedMap<T, U> value) {
		Object2ObjectSortedMap<T, U> map;
		if (value == null) {
			map = this.clearAndGetPrevious();
		} else {
			map = this
					.setAllValuesAndGetPrevious(value.entrySet().stream())
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> a, Object2ObjectLinkedOpenHashMap::new));
		}
		return map;
	}

	@Override
	default boolean setAndGetChanged(@Nullable Object2ObjectSortedMap<T, U> value) {
		if (value != null && value.isEmpty()) {
			value = null;
		}
		var prev = this.setAndGetPrevious(value);
		if (prev == null) {
			return value != null;
		} else {
			return !Objects.equals(prev, value);
		}
	}

	@Override
	default Delta<Object2ObjectSortedMap<T, U>> updateAndGetDelta(
			SerializationFunction<@Nullable Object2ObjectSortedMap<T, U>, @Nullable Object2ObjectSortedMap<T, U>> updater) {
		var updateMode = this.getUpdateMode();
		if (updateMode == UpdateMode.ALLOW_UNSAFE) {
			Object2ObjectSortedMap<T, U> v = this
					.getAllValues(null, true)
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> a, Object2ObjectLinkedOpenHashMap::new));

			if (v.isEmpty()) {
				v = null;
			}

			var result = updater.apply(v);
			if (result != null && result.isEmpty()) {
				result = null;
			}
			this.setAllValues(result != null ? result.entrySet().stream() : null);
			return new Delta<>(v, result);
		} else if (updateMode == UpdateMode.ALLOW) {
			throw new UnsupportedOperationException("Maps can't be updated atomically");
		} else if (updateMode == UpdateMode.DISALLOW) {
			throw new UnsupportedOperationException("Map can't be updated because updates are disabled");
		} else {
			throw new UnsupportedOperationException("Unknown update mode: " + updateMode);
		}
	}

	@Override
	default Object2ObjectSortedMap<T, U> clearAndGetPrevious() {
		return this.setAndGetPrevious(Object2ObjectSortedMaps.emptyMap());
	}

	@Override
	default Object2ObjectSortedMap<T, U> get(@Nullable CompositeSnapshot snapshot) {
		Object2ObjectSortedMap<T, U> map = this
				.getAllValues(snapshot, true)
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> a, Object2ObjectLinkedOpenHashMap::new));
		return map.isEmpty() ? null : map;
	}

	@Override
	default long leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return this.getAllStages(snapshot, false).count();
	}

	/**
	 * Value getter doesn't lock data. Please make sure to lock before getting data.
	 */
	default ValueGetterBlocking<T, U> getDbValueGetter(@Nullable CompositeSnapshot snapshot) {
		return k -> getValue(snapshot, k);
	}

	default ValueGetter<T, U> getAsyncDbValueGetter(@Nullable CompositeSnapshot snapshot) {
		return k -> getValue(snapshot, k);
	}
}
