package it.cavallium.dbengine.database.structures;

import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

public class LLSet implements LLKeyValueDatabaseStructure {

	private static final byte[] EMPTY_VALUE = new byte[0];
	private final LLDictionary dictionary;

	public LLSet(LLDictionary dictionary) {
		this.dictionary = dictionary;
	}

	@Override
	public String getDatabaseName() {
		return dictionary.getDatabaseName();
	}

	private byte[][] generateEmptyArray(int length) {
		byte[][] data = new byte[length][];
		for (int i = 0; i < length; i++) {
			data[i] = EMPTY_VALUE;
		}
		return data;
	}

	public boolean contains(@Nullable LLSnapshot snapshot, byte[] value) throws IOException {
		return dictionary.contains(snapshot, value);
	}

	public boolean add(byte[] value, LLSetResultType resultType) throws IOException {
		Optional<byte[]> response = dictionary.put(value, EMPTY_VALUE, resultType.getDictionaryResultType());
		if (resultType == LLSetResultType.VALUE_CHANGED) {
			return LLUtils.responseToBoolean(response.orElseThrow());
		}
		return false;
	}

	public void addMulti(byte[][] values) throws IOException {
		dictionary.putMulti(values, generateEmptyArray(values.length), LLDictionaryResultType.VOID, (x) -> {});
	}

	public boolean remove(byte[] value, LLSetResultType resultType) throws IOException {
		Optional<byte[]> response = dictionary.remove(value, resultType.getDictionaryResultType());
		if (resultType == LLSetResultType.VALUE_CHANGED) {
			return LLUtils.responseToBoolean(response.orElseThrow());
		}
		return false;
	}

	public void clearUnsafe() throws IOException {
		dictionary.clear();
	}

	public void forEach(@Nullable LLSnapshot snapshot, int parallelism, Consumer<byte[]> consumer) {
		dictionary.forEach(snapshot, parallelism, (key, emptyValue) -> consumer.accept(key));
	}

	public void replaceAll(int parallelism, Function<byte[], byte[]> consumer) throws IOException {
		dictionary.replaceAll(parallelism, true, (key, emptyValue) -> Map.entry(consumer.apply(key), emptyValue));
	}

	public long size(@Nullable LLSnapshot snapshot, boolean fast) throws IOException {
		return dictionary.size(snapshot, fast);
	}

	public boolean isEmptyUnsafe(@Nullable LLSnapshot snapshot) throws IOException {
		return dictionary.isEmpty(snapshot);
	}

	public Optional<byte[]> removeOneUnsafe() throws IOException {
		return dictionary.removeOne().map(Entry::getKey);
	}

	public enum LLSetResultType {
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
}
