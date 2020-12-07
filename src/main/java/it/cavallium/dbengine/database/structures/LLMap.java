package it.cavallium.dbengine.database.structures;

import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.jetbrains.annotations.Nullable;

public class LLMap implements LLKeyValueDatabaseStructure {

	private final LLDictionary dictionary;

	public LLMap(LLDictionary dictionary) {
		this.dictionary = dictionary;
	}

	public Optional<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key) throws IOException {
		return dictionary.get(snapshot, key);
	}

	public Optional<byte[]> put(byte[] key, byte[] value, LLMapResultType resultType)
			throws IOException {
		return dictionary.put(key, value, resultType.getDictionaryResultType());
	}

	public void putMulti(byte[][] key, byte[][] value, LLMapResultType resultType,
			Consumer<Optional<byte[]>> results) throws IOException {
		dictionary.putMulti(key, value, resultType.getDictionaryResultType(),
				(result) -> results.accept(Optional.ofNullable(result.length == 0 ? null : result)));
	}

	public boolean contains(@Nullable LLSnapshot snapshot, byte[] key) throws IOException {
		return dictionary.contains(snapshot, key);
	}

	public Optional<byte[]> remove(byte[] key, LLMapResultType resultType) throws IOException {
		return dictionary.remove(key, resultType.getDictionaryResultType());
	}

	public void clear() throws IOException {
		dictionary.clear();
	}

	public long size(@Nullable LLSnapshot snapshot, boolean fast) throws IOException {
		return dictionary.size(snapshot, fast);
	}

	/**
	 * The consumer can be called from different threads
	 */
	public void forEach(@Nullable LLSnapshot snapshot, int parallelism, BiConsumer<byte[], byte[]> consumer) {
		dictionary.forEach(snapshot, parallelism, consumer);
	}

	/**
	 * The consumer can be called from different threads
	 */
	public void replaceAll(int parallelism, boolean replaceKeys, BiFunction<byte[], byte[], Entry<byte[], byte[]>> consumer) throws IOException {
		dictionary.replaceAll(parallelism, replaceKeys, consumer);
	}

	@Override
	public String getDatabaseName() {
		return dictionary.getDatabaseName();
	}

	public enum LLMapResultType {
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
		return new StringJoiner(", ", LLMap.class.getSimpleName() + "[", "]")
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
		LLMap llMap = (LLMap) o;
		return Objects.equals(dictionary, llMap.dictionary);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dictionary);
	}
}
