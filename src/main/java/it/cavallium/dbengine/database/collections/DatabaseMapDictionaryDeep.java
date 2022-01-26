package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
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

	private static final Logger logger = LogManager.getLogger(DatabaseMapDictionaryDeep.class);

	private static final Drop<DatabaseMapDictionaryDeep<?, ?, ?>> DROP = new Drop<>() {
		@Override
		public void drop(DatabaseMapDictionaryDeep<?, ?, ?> obj) {
			try {
				if (obj.range != null) {
					obj.range.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close range", ex);
			}
			try {
				if (obj.keyPrefix != null) {
					obj.keyPrefix.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close keyPrefix", ex);
			}
			try {
				if (obj.onClose != null) {
					obj.onClose.run();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close onClose", ex);
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
	protected final SubStageGetter<U, US> subStageGetter;
	protected final SerializerFixedBinaryLength<T> keySuffixSerializer;
	protected final int keyPrefixLength;
	protected final int keySuffixLength;
	protected final int keyExtLength;
	protected final Mono<Send<LLRange>> rangeMono;

	protected LLRange range;
	protected Buffer keyPrefix;
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

	static void firstRangeKey(Buffer prefixKey, int prefixLength, int suffixLength, int extLength) {
		zeroFillKeySuffixAndExt(prefixKey, prefixLength, suffixLength, extLength);
	}

	static void nextRangeKey(Buffer prefixKey, int prefixLength, int suffixLength, int extLength) {
		zeroFillKeySuffixAndExt(prefixKey, prefixLength, suffixLength, extLength);
		incrementPrefix(prefixKey, prefixLength);
	}

	protected static void zeroFillKeySuffixAndExt(@NotNull Buffer prefixKey,
			int prefixLength, int suffixLength, int extLength) {
		//noinspection UnnecessaryLocalVariable
		var result = prefixKey;
		assert result.readableBytes() == prefixLength;
		assert suffixLength > 0;
		assert extLength >= 0;
		result.ensureWritable(suffixLength + extLength, suffixLength + extLength, true);
		for (int i = 0; i < suffixLength + extLength; i++) {
			result.writeByte((byte) 0x0);
		}
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
			var firstKey = prefixKey == null ? alloc.allocate(keyPrefixLength + keySuffixLength + keyExtLength)
					: prefixKey.copy();
			try {
				firstRangeKey(firstKey, keyPrefixLength, keySuffixLength, keyExtLength);
				var nextRangeKey = prefixKey == null ? alloc.allocate(keyPrefixLength + keySuffixLength + keyExtLength)
						: prefixKey.copy();
				try {
					nextRangeKey(nextRangeKey, keyPrefixLength, keySuffixLength, keyExtLength);
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
								.<Entry<T, US>>handle((us, sink) -> {
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
		var keyPrefix = this.keyPrefix == null ? null : this.keyPrefix.send();
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
					onClose
			);
			drop.attach(instance);
			return instance;
		};
	}

	@Override
	protected void makeInaccessible() {
		this.keyPrefix = null;
		this.range = null;
		this.onClose = null;
	}
}
