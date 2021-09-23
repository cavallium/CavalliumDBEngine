package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import io.net5.util.IllegalReferenceCountException;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.LiveResourceSupport;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

// todo: implement optimized methods (which?)
public class DatabaseMapDictionaryDeep<T, U, US extends DatabaseStage<U>> extends
		LiveResourceSupport<DatabaseStage<Map<T, U>>, DatabaseMapDictionaryDeep<T, U, US>>
		implements DatabaseStageMap<T, U, US> {

	protected final LLDictionary dictionary;
	private final BufferAllocator alloc;
	protected final SubStageGetter<U, US> subStageGetter;
	protected final SerializerFixedBinaryLength<T> keySuffixSerializer;
	protected final int keyPrefixLength;
	protected final int keySuffixLength;
	protected final int keyExtLength;
	protected final Mono<Send<LLRange>> rangeMono;

	protected LLRange range;
	protected Buffer keyPrefix;

	private static void incrementPrefix(Buffer prefix, int prefixLength) {
		assert prefix.readableBytes() >= prefixLength;
		assert prefix.readerOffset() == 0;
		final var originalKeyLength = prefix.readableBytes();
		boolean overflowed = true;
		final int ff = 0xFF;
		int writtenBytes = 0;
		for (int i = prefixLength - 1; i >= 0; i--) {
			int iByte = prefix.getUnsignedByte(i);
			if (iByte != ff) {
				prefix.setUnsignedByte(i, iByte + 1);
				writtenBytes++;
				overflowed = false;
				break;
			} else {
				prefix.setUnsignedByte(i, 0x00);
				writtenBytes++;
			}
		}
		assert prefixLength - writtenBytes >= 0;

		if (overflowed) {
			assert prefix.writerOffset() == originalKeyLength;
			prefix.ensureWritable(1, 1, true);
			prefix.writerOffset(originalKeyLength + 1);
			for (int i = 0; i < originalKeyLength; i++) {
				prefix.setUnsignedByte(i, 0xFF);
			}
			prefix.setUnsignedByte(originalKeyLength, (byte) 0x00);
		}
	}

	static Buffer firstRangeKey(BufferAllocator alloc, Send<Buffer> prefixKey, int prefixLength, int suffixLength,
			int extLength) {
		return zeroFillKeySuffixAndExt(alloc, prefixKey, prefixLength, suffixLength, extLength);
	}

	static Buffer nextRangeKey(BufferAllocator alloc, Send<Buffer> prefixKey, int prefixLength, int suffixLength,
			int extLength) {
		try (prefixKey) {
			Buffer nonIncremented = zeroFillKeySuffixAndExt(alloc, prefixKey, prefixLength, suffixLength, extLength);
			incrementPrefix(nonIncremented, prefixLength);
			return nonIncremented;
		}
	}

	protected static Buffer zeroFillKeySuffixAndExt(BufferAllocator alloc, @NotNull Send<Buffer> prefixKeySend,
			int prefixLength, int suffixLength, int extLength) {
		var result = prefixKeySend.receive();
		if (result == null) {
			assert prefixLength == 0;
			var buf = alloc.allocate(prefixLength + suffixLength + extLength);
			buf.writerOffset(prefixLength + suffixLength + extLength);
			buf.fill((byte) 0);
			return buf;
		} else {
			assert result.readableBytes() == prefixLength;
			assert suffixLength > 0;
			assert extLength >= 0;
			result.ensureWritable(suffixLength + extLength, suffixLength + extLength, true);
			for (int i = 0; i < suffixLength + extLength; i++) {
				result.writeByte((byte) 0x0);
			}
			return result;
		}
	}

	static Buffer firstRangeKey(BufferAllocator alloc, Send<Buffer> prefixKey, Send<Buffer> suffixKey, int prefixLength,
			int suffixLength, int extLength) {
		return zeroFillKeyExt(alloc, prefixKey, suffixKey, prefixLength, suffixLength, extLength);
	}

	static Buffer nextRangeKey(BufferAllocator alloc, Send<Buffer> prefixKey, Send<Buffer> suffixKey, int prefixLength,
			int suffixLength, int extLength) {
		Buffer nonIncremented = zeroFillKeyExt(alloc, prefixKey, suffixKey, prefixLength, suffixLength, extLength);
		incrementPrefix(nonIncremented, prefixLength + suffixLength);
		return nonIncremented;
	}

	protected static Buffer zeroFillKeyExt(BufferAllocator alloc, Send<Buffer> prefixKeySend, Send<Buffer> suffixKeySend,
			int prefixLength, int suffixLength, int extLength) {
		try (var prefixKey = prefixKeySend.receive()) {
			try (var suffixKey = suffixKeySend.receive()) {
				assert prefixKey.readableBytes() == prefixLength;
				assert suffixKey.readableBytes() == suffixLength;
				assert suffixLength > 0;
				assert extLength >= 0;

				Buffer result = LLUtils.compositeBuffer(alloc, prefixKey.send(), suffixKey.send());
				result.ensureWritable(extLength, extLength, true);
				for (int i = 0; i < extLength; i++) {
					result.writeByte((byte) 0);
				}

				assert result.readableBytes() == prefixLength + suffixLength + extLength;
				return result;
			}
		}
	}

	/**
	 * Use DatabaseMapDictionaryRange.simple instead
	 */
	@Deprecated
	public static <T, U> DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T> keySerializer, SubStageGetterSingle<U> subStageGetter,
			Drop<DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>>> drop) {
		return new DatabaseMapDictionaryDeep<>(dictionary, LLUtils.empty(dictionary.getAllocator()), keySerializer,
				subStageGetter, 0, drop);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepTail(
			LLDictionary dictionary, SerializerFixedBinaryLength<T> keySerializer, int keyExtLength,
			SubStageGetter<U, US> subStageGetter, Drop<DatabaseMapDictionaryDeep<T, U, US>> drop) {
		return new DatabaseMapDictionaryDeep<>(dictionary, LLUtils.empty(dictionary.getAllocator()), keySerializer,
				subStageGetter, keyExtLength, drop);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepIntermediate(
			LLDictionary dictionary, Send<Buffer> prefixKey, SerializerFixedBinaryLength<T> keySuffixSerializer,
			SubStageGetter<U, US> subStageGetter, int keyExtLength, Drop<DatabaseMapDictionaryDeep<T, U, US>> drop) {
		return new DatabaseMapDictionaryDeep<>(dictionary, prefixKey, keySuffixSerializer, subStageGetter,
				keyExtLength, drop);
	}

	protected DatabaseMapDictionaryDeep(LLDictionary dictionary, @NotNull Send<Buffer> prefixKeyToReceive,
			SerializerFixedBinaryLength<T> keySuffixSerializer, SubStageGetter<U, US> subStageGetter, int keyExtLength,
			Drop<DatabaseMapDictionaryDeep<T, U, US>> drop) {
		super(new CloseOnDrop<>(drop));
		try (var prefixKey = prefixKeyToReceive.receive()) {
			this.dictionary = dictionary;
			this.alloc = dictionary.getAllocator();
			this.subStageGetter = subStageGetter;
			this.keySuffixSerializer = keySuffixSerializer;
			assert prefixKey.isAccessible();
			this.keyPrefixLength = prefixKey.readableBytes();
			this.keySuffixLength = keySuffixSerializer.getSerializedBinaryLength();
			this.keyExtLength = keyExtLength;
			Buffer firstKey = firstRangeKey(alloc, LLUtils.copy(alloc, prefixKey), keyPrefixLength,
					keySuffixLength, keyExtLength);
			try (firstKey) {
				var nextRangeKey = nextRangeKey(alloc, LLUtils.copy(alloc, prefixKey),
						keyPrefixLength, keySuffixLength, keyExtLength);
				try (nextRangeKey) {
					assert prefixKey.isAccessible();
					assert keyPrefixLength == 0 || !LLUtils.equals(firstKey, nextRangeKey);
					this.range = keyPrefixLength == 0 ? LLRange.all() : LLRange.of(firstKey.send(), nextRangeKey.send());
					this.rangeMono = LLUtils.lazyRetainRange(this.range);
					assert subStageKeysConsistency(keyPrefixLength + keySuffixLength + keyExtLength);
				}
			}

			this.keyPrefix = prefixKey.send().receive();
		}
	}

	private DatabaseMapDictionaryDeep(LLDictionary dictionary,
			BufferAllocator alloc,
			SubStageGetter<U, US> subStageGetter,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			int keyPrefixLength,
			int keySuffixLength,
			int keyExtLength,
			Mono<Send<LLRange>> rangeMono,
			Send<LLRange> range,
			Send<Buffer> keyPrefix,
			Drop<DatabaseMapDictionaryDeep<T, U, US>> drop) {
		super(new CloseOnDrop<>(drop));
		this.dictionary = dictionary;
		this.alloc = alloc;
		this.subStageGetter = subStageGetter;
		this.keySuffixSerializer = keySuffixSerializer;
		this.keyPrefixLength = keyPrefixLength;
		this.keySuffixLength = keySuffixLength;
		this.keyExtLength = keyExtLength;
		this.rangeMono = rangeMono;

		this.range = range.receive();
		this.keyPrefix = keyPrefix.receive();
	}

	@SuppressWarnings("unused")
	protected boolean suffixKeyConsistency(int keySuffixLength) {
		return this.keySuffixLength == keySuffixLength;
	}

	@SuppressWarnings("unused")
	protected boolean extKeyConsistency(int keyExtLength) {
		return this.keyExtLength == keyExtLength;
	}

	@SuppressWarnings("unused")
	protected boolean suffixAndExtKeyConsistency(int keySuffixAndExtLength) {
		return this.keySuffixLength + this.keyExtLength == keySuffixAndExtLength;
	}

	/**
	 * Removes the prefix from the key
	 */
	protected void removePrefix(Buffer key) {
		assert key.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength
				|| key.readableBytes() == keyPrefixLength + keySuffixLength;
		key.readerOffset(key.readerOffset() + this.keyPrefixLength);
		assert key.readableBytes() == keySuffixLength + keyExtLength
				|| key.readableBytes() == keySuffixLength;
	}

	/**
	 * Removes the ext from the key
	 */
	protected void removeExt(Buffer key) {
		assert key.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength;
		key.writerOffset(keyPrefixLength + keySuffixLength);
		assert key.readableBytes() == keyPrefixLength + keySuffixLength;
	}

	protected Send<Buffer> toKeyWithoutExt(Send<Buffer> suffixKeyToReceive) {
		try (var suffixKey = suffixKeyToReceive.receive()) {
			assert suffixKey.readableBytes() == keySuffixLength;
			try (var result = Objects.requireNonNull(LLUtils.compositeBuffer(alloc,
					LLUtils.copy(alloc, keyPrefix), suffixKey.send()))) {
				assert result.readableBytes() == keyPrefixLength + keySuffixLength;
				return result.send();
			}
		}
	}

	protected LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary.sizeRange(resolveSnapshot(snapshot), rangeMono, fast);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary.isRangeEmpty(resolveSnapshot(snapshot), rangeMono);
	}

	@Override
	public Mono<US> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		var suffixKeyWithoutExt = Mono.fromCallable(() -> toKeyWithoutExt(serializeSuffix(keySuffix)));
		return this.subStageGetter
				.subStage(dictionary, snapshot, suffixKeyWithoutExt)
				.transform(LLUtils::handleDiscard)
				.doOnDiscard(DatabaseStage.class, DatabaseStage::close);
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return dictionary.getUpdateMode();
	}

	@Override
	public Flux<BadBlock> badBlocks() {
		return dictionary.badBlocks(rangeMono);
	}

	@Override
	public Flux<Entry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.getRangeKeyPrefixes(resolveSnapshot(snapshot), rangeMono, keyPrefixLength + keySuffixLength)
				.flatMapSequential(groupKeyWithoutExtSend_ -> Mono.using(
						groupKeyWithoutExtSend_::receive,
						groupKeyWithoutExtSend -> this.subStageGetter
								.subStage(dictionary, snapshot, Mono.fromCallable(() -> groupKeyWithoutExtSend.copy().send()))
								.<Entry<T, US>>handle((us, sink) -> {
									try {
										sink.next(Map.entry(this.deserializeSuffix(getGroupSuffix(groupKeyWithoutExtSend.send())), us));
									} catch (SerializationException ex) {
										sink.error(ex);
									}
								}),
						Resource::close
				))
				.transform(LLUtils::handleDiscard);
	}

	private Send<Buffer> getGroupSuffix(Send<Buffer> groupKeyWithoutExt) {
		try (var buffer = groupKeyWithoutExt.receive()) {
			assert subStageKeysConsistency(buffer.readableBytes() + keyExtLength);
			this.removePrefix(buffer);
			assert subStageKeysConsistency(keyPrefixLength + buffer.readableBytes() + keyExtLength);
			return buffer.send();
		}
	}

	private Send<Buffer> getGroupWithoutExt(Send<Buffer> groupKeyWithExtSend) {
		try (var buffer = groupKeyWithExtSend.receive()) {
			assert subStageKeysConsistency(buffer.readableBytes());
			this.removeExt(buffer);
			assert subStageKeysConsistency(buffer.readableBytes() + keyExtLength);
			return buffer.send();
		}
	}

	private boolean subStageKeysConsistency(int totalKeyLength) {
		if (subStageGetter instanceof SubStageGetterMapDeep) {
			return totalKeyLength
					== keyPrefixLength + keySuffixLength + ((SubStageGetterMapDeep<?, ?, ?>) subStageGetter).getKeyBinaryLength();
		} else if (subStageGetter instanceof SubStageGetterMap) {
			return totalKeyLength
					== keyPrefixLength + keySuffixLength + ((SubStageGetterMap<?, ?>) subStageGetter).getKeyBinaryLength();
		} else {
			return true;
		}
	}

	@Override
	public Flux<Entry<T, U>> setAllValuesAndGetPrevious(Flux<Entry<T, U>> entries) {
		return this
				.getAllValues(null)
				.concatWith(this
						.clear()
						.then(this.putMulti(entries))
						.then(Mono.empty())
				);
	}

	@Override
	public Mono<Void> clear() {
		return Mono
				.defer(() -> {
					if (range.isAll()) {
						return dictionary.clear();
					} else if (range.isSingle()) {
						return dictionary
								.remove(Mono.fromCallable(range::getSingle), LLDictionaryResultType.VOID)
								.doOnNext(Send::close)
								.then();
					} else {
						return dictionary.setRange(rangeMono, Flux.empty());
					}
				});
	}

	//todo: temporary wrapper. convert the whole class to buffers
	protected T deserializeSuffix(@NotNull Send<Buffer> keySuffixToReceive) throws SerializationException {
		try (var keySuffix = keySuffixToReceive.receive()) {
			assert suffixKeyConsistency(keySuffix.readableBytes());
			var result = keySuffixSerializer.deserialize(keySuffix.send());
			assert keyPrefix.isAccessible();
			return result.deserializedData();
		}
	}

	//todo: temporary wrapper. convert the whole class to buffers
	@NotNull
	protected Send<Buffer> serializeSuffix(T keySuffix) throws SerializationException {
		try (var suffixDataToReceive = keySuffixSerializer.serialize(keySuffix)) {
			try (Buffer suffixData = suffixDataToReceive.receive()) {
				assert suffixKeyConsistency(suffixData.readableBytes());
				assert keyPrefix.isAccessible();
				return suffixData.send();
			}
		}
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		throw new IllegalStateException("Closed");
	}

	@Override
	protected Owned<DatabaseMapDictionaryDeep<T, U, US>> prepareSend() {
		var keyPrefix = this.keyPrefix.send();
		var range = this.range.send();
		return drop -> new DatabaseMapDictionaryDeep<>(dictionary, alloc, subStageGetter, keySuffixSerializer,
				keyPrefixLength, keySuffixLength, keyExtLength, rangeMono, range, keyPrefix, drop);
	}

	@Override
	protected void makeInaccessible() {
		this.keyPrefix = null;
		this.range = null;
	}

	private static class CloseOnDrop<T, U, US extends DatabaseStage<U>>
			implements Drop<DatabaseMapDictionaryDeep<T, U, US>> {

		private final Drop<DatabaseMapDictionaryDeep<T,U,US>> delegate;

		public CloseOnDrop(Drop<DatabaseMapDictionaryDeep<T, U, US>> drop) {
			if (drop instanceof CloseOnDrop<T, U, US> closeOnDrop) {
				this.delegate = closeOnDrop.delegate;
			} else {
				this.delegate = drop;
			}
		}

		@Override
		public void drop(DatabaseMapDictionaryDeep<T, U, US> obj) {
			obj.range.close();
			obj.keyPrefix.close();
			delegate.drop(obj);
		}
	}
}
