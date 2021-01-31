package it.cavallium.dbengine.database.collections;

import io.netty.buffer.Unpooled;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class DatabaseSingle<U> implements DatabaseStageEntry<U> {

	private final LLDictionary dictionary;
	private final byte[] key;
	private final Serializer<U> serializer;

	public DatabaseSingle(LLDictionary dictionary, byte[] key, Serializer<U> serializer) {
		this.dictionary = dictionary;
		this.key = key;
		this.serializer = serializer;
	}

	private LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	@Override
	public Mono<U> get(@Nullable CompositeSnapshot snapshot) {
		return dictionary.get(resolveSnapshot(snapshot), key).map(this::deserialize);
	}

	@Override
	public Mono<U> setAndGetPrevious(U value) {
		return dictionary.put(key, serialize(value), LLDictionaryResultType.PREVIOUS_VALUE).map(this::deserialize);
	}

	@Override
	public Mono<U> clearAndGetPrevious() {
		return dictionary.remove(key, LLDictionaryResultType.PREVIOUS_VALUE).map(this::deserialize);
	}

	@Override
	public Mono<Long> size(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(key))
				.map(empty -> empty ? 0L : 1L);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(key));
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private U deserialize(byte[] bytes) {
		var serialized = Unpooled.wrappedBuffer(bytes);
		return serializer.deserialize(serialized);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private byte[] serialize(U bytes) {
		var output = Unpooled.buffer();
		serializer.serialize(bytes, output);
		output.resetReaderIndex();
		int length = output.readableBytes();
		var outputBytes = new byte[length];
		output.getBytes(0, outputBytes, 0, length);
		return outputBytes;
	}
}