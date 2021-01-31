package it.cavallium.dbengine.database.collections;

import io.netty.buffer.Unpooled;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Optimized implementation of "DatabaseMapDictionary with SubStageGetterSingle"
 */
public class DatabaseMapDictionaryRange<T, U> implements DatabaseStageMap<T, U, DatabaseStageEntry<U>> {

	public static final byte[] NO_PREFIX = new byte[0];
	private final LLDictionary dictionary;
	private final byte[] keyPrefix;
	private final FixedLengthSerializer<T> keySuffixSerializer;
	private final LLRange range;
	private final Serializer<U> valueSerializer;

	private static byte[] lastKey(byte[] prefixKey, int prefixLength, int suffixLength) {
		assert prefixKey.length == prefixLength;
		byte[] lastKey = Arrays.copyOf(prefixKey, prefixLength + suffixLength);
		Arrays.fill(lastKey, prefixLength, lastKey.length, (byte) 0xFF);
		return lastKey;
	}

	private static byte[] firstKey(byte[] prefixKey, int prefixLength, int suffixLength) {
		assert prefixKey.length == prefixLength;
		byte[] lastKey = Arrays.copyOf(prefixKey, prefixLength + suffixLength);
		Arrays.fill(lastKey, prefixLength, lastKey.length, (byte) 0x00);
		return lastKey;
	}

	@SuppressWarnings("unused")
	public DatabaseMapDictionaryRange(LLDictionary dictionary, FixedLengthSerializer<T> keySuffixSerializer, Serializer<U> valueSerializer) {
		this(dictionary, NO_PREFIX, keySuffixSerializer, valueSerializer);
	}

	public DatabaseMapDictionaryRange(LLDictionary dictionary, byte[] prefixKey, FixedLengthSerializer<T> keySuffixSerializer, Serializer<U> valueSerializer) {
		this.dictionary = dictionary;
		this.keyPrefix = prefixKey;
		this.keySuffixSerializer = keySuffixSerializer;
		this.valueSerializer = valueSerializer;
		byte[] firstKey = firstKey(keyPrefix, keyPrefix.length, keySuffixSerializer.getLength());
		byte[] lastKey = lastKey(keyPrefix, keyPrefix.length, keySuffixSerializer.getLength());
		this.range = keyPrefix.length == 0 ? LLRange.all() : LLRange.of(firstKey, lastKey);
	}

	private boolean suffixKeyConsistency(int keySuffixLength) {
		return this.keySuffixSerializer.getLength() == keySuffixLength;
	}

	private byte[] toKey(byte[] suffixKey) {
		assert suffixKeyConsistency(suffixKey.length);
		byte[] key = Arrays.copyOf(keyPrefix, keyPrefix.length + suffixKey.length);
		System.arraycopy(suffixKey, 0, key, keyPrefix.length, suffixKey.length);
		return key;
	}

	private byte[] stripPrefix(byte[] key) {
		return Arrays.copyOfRange(key, this.keyPrefix.length, key.length);
	}

	private LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	@Override
	public Mono<Map<T, U>> get(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRange(resolveSnapshot(snapshot), range)
				.map(this::stripPrefix)
				.collectMap(entry -> deserializeSuffix(entry.getKey()), entry -> deserialize(entry.getValue()), HashMap::new);
	}

	@Override
	public Mono<Map<T, U>> setAndGetPrevious(Map<T, U> value) {
		return dictionary
				.setRange(range,
						Flux
								.fromIterable(value.entrySet())
								.map(entry -> Map.entry(serializeSuffix(entry.getKey()), serialize(entry.getValue()))),
						true
				)
				.map(this::stripPrefix)
				.collectMap(entry -> deserializeSuffix(entry.getKey()), entry -> deserialize(entry.getValue()), HashMap::new);
	}

	private Entry<byte[], byte[]> stripPrefix(Entry<byte[], byte[]> entry) {
		byte[] keySuffix = stripPrefix(entry.getKey());
		return Map.entry(keySuffix, entry.getValue());
	}

	@Override
	public Mono<Map<T, U>> clearAndGetPrevious() {
		return dictionary
				.setRange(range, Flux.empty(), true)
				.map(this::stripPrefix)
				.collectMap(entry -> deserializeSuffix(entry.getKey()), entry -> deserialize(entry.getValue()), HashMap::new);
	}

	@Override
	public Mono<Long> size(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary.sizeRange(resolveSnapshot(snapshot), range, true);
	}

	@Override
	public Mono<DatabaseStageEntry<U>> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return Mono.just(new DatabaseSingle<>(dictionary, toKey(serializeSuffix(keySuffix)), Serializer.noopBytes())).map(entry -> new DatabaseSingleMapped<>(entry, valueSerializer));
	}

	@Override
	public Flux<Entry<T, DatabaseStageEntry<U>>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRangeKeys(resolveSnapshot(snapshot), range)
				.map(this::stripPrefix)
				.map(keySuffix -> Map.entry(deserializeSuffix(keySuffix),
						new DatabaseSingleMapped<>(new DatabaseSingle<>(dictionary, toKey(keySuffix), Serializer.noopBytes()),
								valueSerializer
						)
				));
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		var serializedEntries = entries
				.map(entry -> Map.entry(toKey(serializeSuffix(entry.getKey())), serialize(entry.getValue())));
		return dictionary.setRange(range, serializedEntries, true)
				.map(entry -> Map.entry(deserializeSuffix(entry.getKey()), deserialize(entry.getValue())));
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private U deserialize(byte[] bytes) {
		var serialized = Unpooled.wrappedBuffer(bytes);
		return valueSerializer.deserialize(serialized);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private byte[] serialize(U bytes) {
		var output = Unpooled.buffer();
		valueSerializer.serialize(bytes, output);
		output.resetReaderIndex();
		int length = output.readableBytes();
		var outputBytes = new byte[length];
		output.getBytes(0, outputBytes, 0, length);
		return outputBytes;
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private T deserializeSuffix(byte[] keySuffix) {
		var serialized = Unpooled.wrappedBuffer(keySuffix);
		return keySuffixSerializer.deserialize(serialized);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private byte[] serializeSuffix(T keySuffix) {
		var output = Unpooled.buffer(keySuffixSerializer.getLength(), keySuffixSerializer.getLength());
		var outputBytes = new byte[keySuffixSerializer.getLength()];
		keySuffixSerializer.serialize(keySuffix, output);
		output.getBytes(0, outputBytes, 0, keySuffixSerializer.getLength());
		return outputBytes;
	}
}
