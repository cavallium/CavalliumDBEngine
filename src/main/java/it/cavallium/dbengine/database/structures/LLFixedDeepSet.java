package it.cavallium.dbengine.database.structures;

import it.cavallium.dbengine.database.LLDeepDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.unimi.dsi.fastutil.objects.ObjectSets.UnmodifiableSet;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.type.Bytes;
import org.warp.commonutils.type.UnmodifiableIterableMap;
import org.warp.commonutils.type.UnmodifiableIterableSet;
import org.warp.commonutils.type.UnmodifiableMap;

/**
 * A set in which keys and values must have a fixed size
 */
public class LLFixedDeepSet implements LLKeyValueDatabaseStructure {

	private static final byte[] EMPTY_VALUE = new byte[0];
	private static final Bytes EMPTY_VALUE_BYTES = new Bytes(EMPTY_VALUE);
	private final LLDeepDictionary dictionary;

	public LLFixedDeepSet(LLDeepDictionary dictionary) {
		this.dictionary = dictionary;
	}

	private byte[][] generateEmptyArray(int length) {
		byte[][] data = new byte[length][];
		for (int i = 0; i < length; i++) {
			data[i] = EMPTY_VALUE;
		}
		return data;
	}

	private Bytes[] generateEmptyBytesArray(int length) {
		Bytes[] data = new Bytes[length];
		for (int i = 0; i < length; i++) {
			data[i] = EMPTY_VALUE_BYTES;
		}
		return data;
	}

	public UnmodifiableIterableSet<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key1) throws IOException {
		return dictionary.get(snapshot, key1).toUnmodifiableIterableKeysSet(byte[][]::new);
	}

	public boolean contains(@Nullable LLSnapshot snapshot, byte[] key1, byte[] value) throws IOException {
		return dictionary.contains(snapshot, key1, value);
	}

	public boolean isEmpty(@Nullable LLSnapshot snapshot, byte[] key1) {
		return dictionary.isEmpty(snapshot, key1);
	}

	public boolean add(byte[] key1, byte[] value, LLDeepSetItemResultType resultType) throws IOException {
		Optional<byte[]> response = dictionary.put(key1, value, EMPTY_VALUE, resultType.getDictionaryResultType());
		if (resultType == LLDeepSetItemResultType.VALUE_CHANGED) {
			return LLUtils.responseToBoolean(response.orElseThrow());
		}
		return false;
	}

	public void addMulti(byte[] key1, byte[][] values) throws IOException {
		dictionary.putMulti(key1, values, generateEmptyArray(values.length), LLDictionaryResultType.VOID, (x) -> {});
	}

	/**
	 * Note: this will remove previous elements because it replaces the entire set
	 */
	public void put(byte[] key1, UnmodifiableIterableSet<byte[]> values) throws IOException {
		dictionary.put(key1, values.toUnmodifiableIterableMapSetValues(generateEmptyArray(values.size())));
	}

	public void putMulti(byte[][] keys1, UnmodifiableIterableSet<byte[]>[] values) throws IOException {
		var fixedValues = new UnmodifiableIterableMap[values.length];
		for (int i = 0; i < values.length; i++) {
			fixedValues[i] = values[i].toUnmodifiableIterableMapSetValues(generateEmptyArray(values[i].size()));
		}
		//noinspection unchecked
		dictionary.putMulti(keys1, fixedValues);
	}

	public void clear() throws IOException {
		dictionary.clear();
	}

	public Optional<UnmodifiableIterableSet<byte[]>> clear(byte[] key1, LLDeepSetResultType resultType) throws IOException {
		Optional<UnmodifiableIterableMap<byte[], byte[]>> response = dictionary.clear(key1, resultType.getDictionaryResultType());
		if (response.isEmpty()) {
			return Optional.empty();
		} else {
			return Optional.of(response.get().toUnmodifiableIterableKeysSet(byte[][]::new));
		}
	}

	public boolean remove(byte[] key1, byte[] value, LLDeepSetItemResultType resultType) throws IOException {
		Optional<byte[]> response = dictionary.remove(key1, value, resultType.getDictionaryResultType());
		if (resultType == LLDeepSetItemResultType.VALUE_CHANGED) {
			return LLUtils.responseToBoolean(response.orElseThrow());
		}
		return false;
	}

	public void forEach(@Nullable LLSnapshot snapshot, int parallelism, BiConsumer<byte[], UnmodifiableIterableSet<byte[]>> consumer) {
		dictionary.forEach(snapshot, parallelism, (key1, entries) -> consumer.accept(key1, entries.toUnmodifiableIterableKeysSet(byte[][]::new)));
	}

	public void forEach(@Nullable LLSnapshot snapshot, int parallelism, byte[] key1, Consumer<byte[]> consumer) {
		dictionary.forEach(snapshot, parallelism, key1, (value, empty) -> consumer.accept(value));
	}

	public void replaceAll(int parallelism, BiFunction<byte[], UnmodifiableIterableSet<byte[]>, Entry<byte[], UnmodifiableSet<Bytes>>> consumer) throws IOException {
		dictionary.replaceAll(parallelism, true, (key1, entries) -> {
			var result = consumer.apply(key1, entries.toUnmodifiableIterableKeysSet(byte[][]::new));
			var resultItems = result.getValue().toArray(Bytes[]::new);
			return Map.entry(result.getKey(), UnmodifiableMap.of(resultItems, generateEmptyArray(resultItems.length)));
		});
	}

	public void replaceAll(int parallelism, byte[] key1, Function<byte[], byte[]> consumer) throws IOException {
		dictionary.replaceAll(parallelism, true, key1, (value, empty) -> {
			var changedValue = consumer.apply(value);
			return Map.entry(changedValue, EMPTY_VALUE);
		});
	}

	public long size(@Nullable LLSnapshot snapshot, boolean fast) throws IOException {
		return dictionary.size(snapshot, fast);
	}

	public long exactSize(@Nullable LLSnapshot snapshot, byte[] key1) {
		return dictionary.exactSize(snapshot, key1);
	}

	@Override
	public String getDatabaseName() {
		return dictionary.getDatabaseName();
	}

	public enum LLDeepSetResultType {
		VOID,
		VALUE_CHANGED,
		PREVIOUS_VALUE;

		public LLDictionaryResultType getDictionaryResultType() {
			switch (this) {
				case VOID:
					return LLDictionaryResultType.VOID;
				case VALUE_CHANGED:
					return LLDictionaryResultType.VALUE_CHANGED;
				case PREVIOUS_VALUE:
					return LLDictionaryResultType.PREVIOUS_VALUE;
			}

			return LLDictionaryResultType.VOID;
		}
	}

	public enum LLDeepSetItemResultType {
		VOID,
		VALUE_CHANGED;

		public LLDictionaryResultType getDictionaryResultType() {
			switch (this) {
				case VOID:
					return LLDictionaryResultType.VOID;
				case VALUE_CHANGED:
					return LLDictionaryResultType.VALUE_CHANGED;
			}

			return LLDictionaryResultType.VOID;
		}
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLFixedDeepSet.class.getSimpleName() + "[", "]")
				.add("dictionary=" + dictionary)
				.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLFixedDeepSet llMap = (LLFixedDeepSet) o;
		return Objects.equals(dictionary, llMap.dictionary);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dictionary);
	}
}
