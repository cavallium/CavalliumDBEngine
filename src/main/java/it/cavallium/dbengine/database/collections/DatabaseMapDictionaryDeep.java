package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.DefaultBufferAllocators;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.Resource;
import io.netty5.buffer.api.Send;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

// todo: implement optimized methods (which?)
public class DatabaseMapDictionaryDeep<T, U, US extends DatabaseStage<U>> extends
		ResourceSupport<DatabaseStage<Object2ObjectSortedMap<T, U>>, DatabaseMapDictionaryDeep<T, U, US>> implements
		DatabaseStageMap<T, U, US> {

	private static final Logger LOG = LogManager.getLogger(DatabaseMapDictionaryDeep.class);

	private static final Drop<DatabaseMapDictionaryDeep<?, ?, ?>> DROP = new Drop<>() {
		@Override
		public void drop(DatabaseMapDictionaryDeep<?, ?, ?> obj) {
			try {
				if (obj.range != null) {
					obj.range.close();
				}
			} catch (Throwable ex) {
				LOG.error("Failed to close range", ex);
			}
			try {
				if (obj.keyPrefix != null) {
					obj.keyPrefix.close();
				}
			} catch (Throwable ex) {
				LOG.error("Failed to close keyPrefix", ex);
			}
			try {
				if (obj.keySuffixAndExtZeroBuffer != null) {
					obj.keySuffixAndExtZeroBuffer.close();
				}
			} catch (Throwable ex) {
				LOG.error("Failed to close keySuffixAndExtZeroBuffer", ex);
			}
			try {
				if (obj.onClose != null) {
					obj.onClose.run();
				}
			} catch (Throwable ex) {
				LOG.error("Failed to close onClose", ex);
			}
		}

		@Override
		public Drop<DatabaseMapDictionaryDeep<?, ?, ?>> fork() {
			return this;
		}

		@Override
		public void attach(DatabaseMapDictionaryDeep<?, ?, ?> obj) {

		}
	};

	protected final LLDictionary dictionary;
	private final BufferAllocator alloc;
	private final AtomicLong totalZeroBytesErrors = new AtomicLong();
	protected final SubStageGetter<U, US> subStageGetter;
	protected final SerializerFixedBinaryLength<T> keySuffixSerializer;
	protected final int keyPrefixLength;
	protected final int keySuffixLength;
	protected final int keyExtLength;
	protected final Mono<Send<LLRange>> rangeMono;

	protected LLRange range;
	protected Buffer keyPrefix;
	protected Buffer keySuffixAndExtZeroBuffer;
	protected Runnable onClose;

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

	static void firstRangeKey(Buffer prefixKey, int prefixLength, Buffer suffixAndExtZeroes) {
		zeroFillKeySuffixAndExt(prefixKey, prefixLength, suffixAndExtZeroes);
	}

	static void nextRangeKey(Buffer prefixKey, int prefixLength, Buffer suffixAndExtZeroes) {
		zeroFillKeySuffixAndExt(prefixKey, prefixLength, suffixAndExtZeroes);
		incrementPrefix(prefixKey, prefixLength);
	}

	@Deprecated
	static void firstRangeKey(Buffer prefixKey, int prefixLength, int suffixLength, int extLength) {
		try (var zeroBuf = DefaultBufferAllocators.offHeapAllocator().allocate(suffixLength + extLength)) {
			zeroBuf.fill((byte) 0);
			zeroBuf.writerOffset(suffixLength + extLength);
			zeroFillKeySuffixAndExt(prefixKey, prefixLength, zeroBuf);
		}
	}

	@Deprecated
	static void nextRangeKey(Buffer prefixKey, int prefixLength, int suffixLength, int extLength) {
		try (var zeroBuf = DefaultBufferAllocators.offHeapAllocator().allocate(suffixLength + extLength)) {
			zeroBuf.fill((byte) 0);
			zeroBuf.writerOffset(suffixLength + extLength);
			zeroFillKeySuffixAndExt(prefixKey, prefixLength, zeroBuf);
			incrementPrefix(prefixKey, prefixLength);
		}
	}

	protected static void zeroFillKeySuffixAndExt(@NotNull Buffer prefixKey,
			int prefixLength, Buffer suffixAndExtZeroes) {
		//noinspection UnnecessaryLocalVariable
		var result = prefixKey;
		var suffixLengthAndExtLength = suffixAndExtZeroes.readableBytes();
		assert result.readableBytes() == prefixLength;
		assert suffixLengthAndExtLength > 0 : "Suffix length + ext length is < 0: " + suffixLengthAndExtLength;
		prefixKey.ensureWritable(suffixLengthAndExtLength);
		suffixAndExtZeroes.copyInto(suffixAndExtZeroes.readerOffset(),
				prefixKey,
				prefixKey.writerOffset(),
				suffixLengthAndExtLength
		);
		prefixKey.skipWritable(suffixLengthAndExtLength);
	}

	/**
	 * Use DatabaseMapDictionaryRange.simple instead
	 */
	@Deprecated
	public static <T, U> DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T> keySerializer, SubStageGetterSingle<U> subStageGetter,
			Runnable onClose) {
		return new DatabaseMapDictionaryDeep<>(dictionary, null, keySerializer,
				subStageGetter, 0, onClose);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepTail(
			LLDictionary dictionary, SerializerFixedBinaryLength<T> keySerializer, int keyExtLength,
			SubStageGetter<U, US> subStageGetter, Runnable onClose) {
		return new DatabaseMapDictionaryDeep<>(dictionary, null, keySerializer,
				subStageGetter, keyExtLength, onClose);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepIntermediate(
			LLDictionary dictionary, Buffer prefixKey, SerializerFixedBinaryLength<T> keySuffixSerializer,
			SubStageGetter<U, US> subStageGetter, int keyExtLength, Runnable onClose) {
		return new DatabaseMapDictionaryDeep<>(dictionary, prefixKey, keySuffixSerializer, subStageGetter,
				keyExtLength, onClose);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	protected DatabaseMapDictionaryDeep(LLDictionary dictionary, @Nullable Buffer prefixKey,
			SerializerFixedBinaryLength<T> keySuffixSerializer, SubStageGetter<U, US> subStageGetter, int keyExtLength,
			Runnable onClose) {
		super((Drop<DatabaseMapDictionaryDeep<T, U, US>>) (Drop) DROP);
		try {
			this.dictionary = dictionary;
			this.alloc = dictionary.getAllocator();
			this.subStageGetter = subStageGetter;
			this.keySuffixSerializer = keySuffixSerializer;
			assert prefixKey == null || prefixKey.isAccessible();
			this.keyPrefixLength = prefixKey == null ? 0 : prefixKey.readableBytes();
			this.keySuffixLength = keySuffixSerializer.getSerializedBinaryLength();
			this.keyExtLength = keyExtLength;
			this.keySuffixAndExtZeroBuffer = alloc
					.allocate(keySuffixLength + keyExtLength)
					.fill((byte) 0)
					.writerOffset(keySuffixLength + keyExtLength)
					.makeReadOnly();
			assert keySuffixAndExtZeroBuffer.readableBytes() == keySuffixLength + keyExtLength :
					"Key suffix and ext zero buffer readable length is not equal"
							+ " to the key suffix length + key ext length. keySuffixAndExtZeroBuffer="
							+ keySuffixAndExtZeroBuffer.readableBytes() + " keySuffixLength=" + keySuffixLength + " keyExtLength="
							+ keyExtLength;
			assert keySuffixAndExtZeroBuffer.readableBytes() > 0;
			var firstKey = prefixKey == null ? alloc.allocate(keyPrefixLength + keySuffixLength + keyExtLength)
					: prefixKey.copy();
			try {
				firstRangeKey(firstKey, keyPrefixLength, keySuffixAndExtZeroBuffer);
				var nextRangeKey = prefixKey == null ? alloc.allocate(keyPrefixLength + keySuffixLength + keyExtLength)
						: prefixKey.copy();
				try {
					nextRangeKey(nextRangeKey, keyPrefixLength, keySuffixAndExtZeroBuffer);
					assert prefixKey == null || prefixKey.isAccessible();
					assert keyPrefixLength == 0 || !LLUtils.equals(firstKey, nextRangeKey);
					if (keyPrefixLength == 0) {
						this.range = LLRange.all();
						firstKey.close();
						nextRangeKey.close();
					} else {
						this.range = LLRange.ofUnsafe(firstKey, nextRangeKey);
					}
					this.rangeMono = LLUtils.lazyRetainRange(this.range);
					assert subStageKeysConsistency(keyPrefixLength + keySuffixLength + keyExtLength);
				} catch (Throwable t) {
					nextRangeKey.close();
					throw t;
				}
			} catch (Throwable t) {
				firstKey.close();
				throw t;
			}

			this.keyPrefix = prefixKey;
			this.onClose = onClose;
		} catch (Throwable t) {
			if (this.keySuffixAndExtZeroBuffer != null && keySuffixAndExtZeroBuffer.isAccessible()) {
				keySuffixAndExtZeroBuffer.close();
			}
			if (prefixKey != null && prefixKey.isAccessible()) {
				prefixKey.close();
			}
			throw t;
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
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
			Send<Buffer> keySuffixAndExtZeroBuffer,
			Runnable onClose) {
		super((Drop<DatabaseMapDictionaryDeep<T,U,US>>) (Drop) DROP);
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
		this.keySuffixAndExtZeroBuffer = keySuffixAndExtZeroBuffer.receive();
		this.onClose = onClose;
	}

	@SuppressWarnings("unused")
	protected boolean suffixKeyLengthConsistency(int keySuffixLength) {
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
	 * @return the prefix
	 */
	protected Buffer splitPrefix(Buffer key) {
		assert key.readableBytes() == keyPrefixLength + keySuffixLength + keyExtLength
				|| key.readableBytes() == keyPrefixLength + keySuffixLength;
		var prefix = key.readSplit(this.keyPrefixLength);
		assert key.readableBytes() == keySuffixLength + keyExtLength
				|| key.readableBytes() == keySuffixLength;
		return prefix;
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
		return dictionary.isRangeEmpty(resolveSnapshot(snapshot), rangeMono, false);
	}

	@Override
	public Mono<US> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		var suffixKeyWithoutExt = Mono.fromCallable(() -> {
			try (var keyWithoutExtBuf = keyPrefix == null
					? alloc.allocate(keySuffixLength + keyExtLength) : keyPrefix.copy()) {
				keyWithoutExtBuf.ensureWritable(keySuffixLength + keyExtLength);
				serializeSuffix(keySuffix, keyWithoutExtBuf);
				return keyWithoutExtBuf.send();
			}
		});
		return this.subStageGetter
				.subStage(dictionary, snapshot, suffixKeyWithoutExt);
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
								.handle((us, sink) -> {
									T deserializedSuffix;
									try (var splittedGroupSuffix = splitGroupSuffix(groupKeyWithoutExtSend)) {
										deserializedSuffix = this.deserializeSuffix(splittedGroupSuffix);
										sink.next(Map.entry(deserializedSuffix, us));
									} catch (SerializationException ex) {
										sink.error(ex);
									}
								}),
						Resource::close
				));
	}

	/**
	 * Split the input. The input will become the ext, the returned data will be the group suffix
	 * @param groupKey group key, will become ext
	 * @return group suffix
	 */
	private Buffer splitGroupSuffix(@NotNull Buffer groupKey) {
		assert subStageKeysConsistency(groupKey.readableBytes())
				|| subStageKeysConsistency(groupKey.readableBytes() + keyExtLength);
		this.splitPrefix(groupKey).close();
		assert subStageKeysConsistency(keyPrefixLength + groupKey.readableBytes())
				|| subStageKeysConsistency(keyPrefixLength + groupKey.readableBytes() + keyExtLength);
		return groupKey.readSplit(keySuffixLength);
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

	protected T deserializeSuffix(@NotNull Buffer keySuffix) throws SerializationException {
		assert suffixKeyLengthConsistency(keySuffix.readableBytes());
		var result = keySuffixSerializer.deserialize(keySuffix);
		assert keyPrefix == null || keyPrefix.isAccessible();
		return result;
	}

	protected void serializeSuffix(T keySuffix, Buffer output) throws SerializationException {
		output.ensureWritable(keySuffixLength);
		var beforeWriterOffset = output.writerOffset();
		keySuffixSerializer.serialize(keySuffix, output);
		var afterWriterOffset = output.writerOffset();
		assert suffixKeyLengthConsistency(afterWriterOffset - beforeWriterOffset);
		assert keyPrefix == null || keyPrefix.isAccessible();
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		throw new IllegalStateException("Closed");
	}

	@Override
	protected Owned<DatabaseMapDictionaryDeep<T, U, US>> prepareSend() {
		var keyPrefix = this.keyPrefix.send();
		var keySuffixAndExtZeroBuffer = this.keySuffixAndExtZeroBuffer.send();
		var range = this.range.send();
		var onClose = this.onClose;
		return drop -> {
			var instance = new DatabaseMapDictionaryDeep<>(dictionary,
					alloc,
					subStageGetter,
					keySuffixSerializer,
					keyPrefixLength,
					keySuffixLength,
					keyExtLength,
					rangeMono,
					range,
					keyPrefix,
					keySuffixAndExtZeroBuffer,
					onClose
			);
			drop.attach(instance);
			return instance;
		};
	}

	@Override
	protected void makeInaccessible() {
		this.keyPrefix = null;
		this.keySuffixAndExtZeroBuffer = null;
		this.range = null;
		this.onClose = null;
	}

	public static <K1, K2, V, R> Flux<R> getAllLeaves2(DatabaseMapDictionaryDeep<K1, Object2ObjectSortedMap<K2, V>, DatabaseMapDictionary<K2, V>> deepMap,
			CompositeSnapshot snapshot,
			TriFunction<K1, K2, V, R> merger) {
		if (deepMap.subStageGetter instanceof SubStageGetterMap<K2, V> subStageGetterMap) {
			var keySuffix1Serializer = deepMap.keySuffixSerializer;
			var keySuffix2Serializer = subStageGetterMap.keySerializer;
			var valueSerializer = subStageGetterMap.valueSerializer;
			return deepMap
					.dictionary
					.getRange(deepMap.resolveSnapshot(snapshot), deepMap.rangeMono)
					.handle((entrySend, sink) -> {
						K1 key1 = null;
						K2 key2 = null;
						try (var entry = entrySend.receive()) {
							var keyBuf = entry.getKeyUnsafe();
							var valueBuf = entry.getValueUnsafe();
							try {
								assert keyBuf != null;
								keyBuf.skipReadable(deepMap.keyPrefixLength);
								try (var key1Buf = keyBuf.split(deepMap.keySuffixLength)) {
									key1 = keySuffix1Serializer.deserialize(key1Buf);
								}
								key2 = keySuffix2Serializer.deserialize(keyBuf);
								assert valueBuf != null;
								var value = valueSerializer.deserialize(valueBuf);
								sink.next(merger.apply(key1, key2, value));
							} catch (IndexOutOfBoundsException ex) {
								var exMessage = ex.getMessage();
								if (exMessage != null && exMessage.contains("read 0 to 0, write 0 to ")) {
									var totalZeroBytesErrors = deepMap.totalZeroBytesErrors.incrementAndGet();
									if (totalZeroBytesErrors < 512 || totalZeroBytesErrors % 10000 == 0) {
										LOG.error("Unexpected zero-bytes value at " + deepMap.dictionary.getDatabaseName()
												+ ":" + deepMap.dictionary.getColumnName()
												+ ":[" + key1
												+ ":" + key2
												+ "](" + LLUtils.toStringSafe(keyBuf) + ") total=" + totalZeroBytesErrors);
									}
									sink.complete();
								} else {
									sink.error(ex);
								}
							}
						} catch (SerializationException ex) {
							sink.error(ex);
						}
					});
		} else {
			throw new IllegalArgumentException();
		}
	}
}
