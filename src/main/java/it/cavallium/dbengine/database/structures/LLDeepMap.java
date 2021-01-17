package it.cavallium.dbengine.database.structures;

import it.cavallium.dbengine.database.LLDeepDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.functional.CancellableBiConsumer;
import org.warp.commonutils.functional.CancellableBiFunction;
import org.warp.commonutils.functional.CancellableTriConsumer;
import org.warp.commonutils.functional.CancellableTriFunction;
import org.warp.commonutils.functional.ConsumerResult;
import org.warp.commonutils.type.Bytes;
import org.warp.commonutils.type.UnmodifiableIterableMap;
import org.warp.commonutils.type.UnmodifiableMap;

public class LLDeepMap implements LLKeyValueDatabaseStructure {

	private final LLDeepDictionary dictionary;

	public LLDeepMap(LLDeepDictionary dictionary) {
		this.dictionary = dictionary;
	}

	public UnmodifiableIterableMap<byte[], byte[]> get(@Nullable LLSnapshot snapshot, byte[] key) throws IOException {
		return dictionary.get(snapshot, key);
	}

	public Optional<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key1, byte[] key2) throws IOException {
		return dictionary.get(snapshot, key1, key2);
	}

	public boolean isEmpty(@Nullable LLSnapshot snapshot, byte[] key1) {
		return dictionary.isEmpty(snapshot, key1);
	}

	public boolean contains(@Nullable LLSnapshot snapshot, byte[] key1, byte[] key2) throws IOException {
		return dictionary.contains(snapshot, key1, key2);
	}

	/**
	 * Note: this will remove previous elements because it replaces the entire map of key
	 */
	public void put(byte[] key1, UnmodifiableIterableMap<byte[], byte[]> value) throws IOException {
		dictionary.put(key1, value);
	}

	public Optional<byte[]> put(byte[] key1, byte[] key2, byte[] value, LLDeepMapResultType resultType) throws IOException {
		return dictionary.put(key1, key2, value, resultType.getDictionaryResultType());
	}

	public void putMulti(byte[][] keys1, UnmodifiableIterableMap<byte[], byte[]>[] values) throws IOException {
		dictionary.putMulti(keys1, values);
	}

	public void putMulti(byte[] key1, byte[][] keys2, byte[][] values, LLDeepMapResultType resultType, Consumer<byte[]> responses) throws IOException {
		dictionary.putMulti(key1, keys2, values, resultType.getDictionaryResultType(), responses);
	}

	public void putMulti(byte[][] keys1, byte[][] keys2, byte[][] values, LLDeepMapResultType resultType, Consumer<byte[]> responses) throws IOException {
		dictionary.putMulti(keys1, keys2, values, resultType.getDictionaryResultType(), responses);
	}

	public void clear() throws IOException {
		dictionary.clear();
	}

	public Optional<UnmodifiableIterableMap<byte[], byte[]>> clear(byte[] key1, LLDeepMapResultType resultType) throws IOException {
		return dictionary.clear(key1, resultType.getDictionaryResultType());
	}

	public Optional<byte[]> remove(byte[] key1, byte[] key2, LLDeepMapResultType resultType) throws IOException {
		return dictionary.remove(key1, key2, resultType.getDictionaryResultType());
	}

	public ConsumerResult forEach(@Nullable LLSnapshot snapshot, int parallelism, CancellableBiConsumer<byte[], UnmodifiableIterableMap<byte[], byte[]>> consumer) {
		return dictionary.forEach(snapshot, parallelism, consumer);
	}

	public ConsumerResult forEach(@Nullable LLSnapshot snapshot, int parallelism, byte[] key1, CancellableBiConsumer<byte[], byte[]> consumer) {
		return dictionary.forEach(snapshot, parallelism, key1, consumer);
	}

	public void replaceAll(int parallelism, boolean replaceKeys, CancellableBiFunction<byte[], UnmodifiableIterableMap<byte[], byte[]>, Entry<byte[], UnmodifiableMap<Bytes, byte[]>>> consumer) throws IOException {
		dictionary.replaceAll(parallelism, replaceKeys, consumer);
	}

	public void replaceAll(int parallelism, boolean replaceKeys, byte[] key1, CancellableBiFunction<byte[], byte[], Entry<byte[], byte[]>> consumer) throws IOException {
		dictionary.replaceAll(parallelism, replaceKeys, key1, consumer);
	}

	public ConsumerResult forEach(@Nullable LLSnapshot snapshot, int parallelism, CancellableTriConsumer<byte[], byte[], byte[]> consumer) {
		return dictionary.forEach(snapshot, parallelism, consumer);
	}

	public void replaceAll(int parallelism, boolean replaceKeys, CancellableTriFunction<byte[], byte[], byte[], ImmutableTriple<byte[], byte[], byte[]>> consumer) throws IOException {
		dictionary.replaceAll(parallelism, replaceKeys, consumer);
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

	public enum LLDeepMapResultType {
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

	@Override
	public String toString() {
		return new StringJoiner(", ", LLDeepMap.class.getSimpleName() + "[", "]")
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
		LLDeepMap llMap = (LLDeepMap) o;
		return Objects.equals(dictionary, llMap.dictionary);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dictionary);
	}
}
