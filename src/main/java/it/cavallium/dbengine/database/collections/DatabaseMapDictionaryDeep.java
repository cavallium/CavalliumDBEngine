package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.DefaultBufferAllocators;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.util.Resource;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.RangeSupplier;
import it.cavallium.dbengine.database.SubStageEntry;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.cavallium.dbengine.utils.SimpleResource;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

// todo: implement optimized methods (which?)
public class DatabaseMapDictionaryDeep<T, U, US extends DatabaseStage<U>> extends SimpleResource implements
		DatabaseStageMap<T, U, US> {

	private static final Logger LOG = LogManager.getLogger(DatabaseMapDictionaryDeep.class);

	protected final LLDictionary dictionary;
	protected final BufferAllocator alloc;
	private final AtomicLong totalZeroBytesErrors = new AtomicLong();
	protected final SubStageGetter<U, US> subStageGetter;
	protected final SerializerFixedBinaryLength<T> keySuffixSerializer;
	protected final int keyPrefixLength;
	protected final int keySuffixLength;
	protected final int keyExtLength;
	protected final Mono<LLRange> rangeMono;

	protected RangeSupplier rangeSupplier;
	protected BufSupplier keyPrefixSupplier;

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
		prefixKey.skipWritableBytes(suffixLengthAndExtLength);
	}

	/**
	 * Use DatabaseMapDictionaryRange.simple instead
	 */
	@Deprecated
	public static <T, U> DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T> keySerializer, SubStageGetterSingle<U> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary, null, keySerializer, subStageGetter, 0);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepTail(
			LLDictionary dictionary, SerializerFixedBinaryLength<T> keySerializer, int keyExtLength,
			SubStageGetter<U, US> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary, null, keySerializer, subStageGetter, keyExtLength);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepIntermediate(
			LLDictionary dictionary, BufSupplier prefixKey, SerializerFixedBinaryLength<T> keySuffixSerializer,
			SubStageGetter<U, US> subStageGetter, int keyExtLength) {
		return new DatabaseMapDictionaryDeep<>(dictionary, prefixKey, keySuffixSerializer, subStageGetter, keyExtLength);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	protected DatabaseMapDictionaryDeep(LLDictionary dictionary, @Nullable BufSupplier prefixKeySupplier,
			SerializerFixedBinaryLength<T> keySuffixSerializer, SubStageGetter<U, US> subStageGetter, int keyExtLength) {
		try (var prefixKey = prefixKeySupplier != null ? prefixKeySupplier.get() : null) {
			this.dictionary = dictionary;
			this.alloc = dictionary.getAllocator();
			this.subStageGetter = subStageGetter;
			this.keySuffixSerializer = keySuffixSerializer;
			this.keyPrefixLength = prefixKey != null ? prefixKey.readableBytes() : 0;
			this.keySuffixLength = keySuffixSerializer.getSerializedBinaryLength();
			this.keyExtLength = keyExtLength;
			try (var keySuffixAndExtZeroBuffer = alloc
					.allocate(keySuffixLength + keyExtLength)
					.fill((byte) 0)
					.writerOffset(keySuffixLength + keyExtLength)
					.makeReadOnly()) {
				assert keySuffixAndExtZeroBuffer.readableBytes() == keySuffixLength + keyExtLength :
						"Key suffix and ext zero buffer readable length is not equal"
								+ " to the key suffix length + key ext length. keySuffixAndExtZeroBuffer="
								+ keySuffixAndExtZeroBuffer.readableBytes() + " keySuffixLength=" + keySuffixLength + " keyExtLength="
								+ keyExtLength;
				assert keySuffixAndExtZeroBuffer.readableBytes() > 0;
				var firstKey = prefixKey != null ? prefixKeySupplier.get()
						: alloc.allocate(keyPrefixLength + keySuffixLength + keyExtLength);
				try {
					firstRangeKey(firstKey, keyPrefixLength, keySuffixAndExtZeroBuffer);
					var nextRangeKey = prefixKey != null ? prefixKeySupplier.get()
							: alloc.allocate(keyPrefixLength + keySuffixLength + keyExtLength);
					try {
						nextRangeKey(nextRangeKey, keyPrefixLength, keySuffixAndExtZeroBuffer);
						assert prefixKey == null || prefixKey.isAccessible();
						assert keyPrefixLength == 0 || !LLUtils.equals(firstKey, nextRangeKey);
						if (keyPrefixLength == 0) {
							this.rangeSupplier = RangeSupplier.ofOwned(LLRange.all());
							firstKey.close();
							nextRangeKey.close();
						} else {
							this.rangeSupplier = RangeSupplier.ofOwned(LLRange.ofUnsafe(firstKey, nextRangeKey));
						}
						this.rangeMono = Mono.fromSupplier(rangeSupplier);
						assert subStageKeysConsistency(keyPrefixLength + keySuffixLength + keyExtLength);
					} catch (Throwable t) {
						nextRangeKey.close();
						throw t;
					}
				} catch (Throwable t) {
					firstKey.close();
					throw t;
				}

				this.keyPrefixSupplier = prefixKeySupplier;
			}
		} catch (Throwable t) {
			if (prefixKeySupplier != null) {
				prefixKeySupplier.close();
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
			Mono<LLRange> rangeMono,
			RangeSupplier rangeSupplier,
			BufSupplier keyPrefixSupplier,
			Runnable onClose) {
		this.dictionary = dictionary;
		this.alloc = alloc;
		this.subStageGetter = subStageGetter;
		this.keySuffixSerializer = keySuffixSerializer;
		this.keyPrefixLength = keyPrefixLength;
		this.keySuffixLength = keySuffixLength;
		this.keyExtLength = keyExtLength;
		this.rangeMono = rangeMono;

		this.rangeSupplier = rangeSupplier;
		this.keyPrefixSupplier = keyPrefixSupplier;
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
			var keyWithoutExtBuf = keyPrefixSupplier == null
				? alloc.allocate(keySuffixLength + keyExtLength) : keyPrefixSupplier.get();
			try {
				keyWithoutExtBuf.ensureWritable(keySuffixLength + keyExtLength);
				serializeSuffix(keySuffix, keyWithoutExtBuf);
			} catch (Throwable ex) {
				keyWithoutExtBuf.close();
				throw ex;
			}
			return keyWithoutExtBuf;
		});
		return this.subStageGetter.subStage(dictionary, snapshot, suffixKeyWithoutExt);
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
	public Flux<SubStageEntry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot, boolean smallRange) {
		return dictionary
				.getRangeKeyPrefixes(resolveSnapshot(snapshot), rangeMono, keyPrefixLength + keySuffixLength, smallRange)
				.flatMapSequential(groupKeyWithoutExt -> this.subStageGetter
						.subStage(dictionary, snapshot, Mono.fromCallable(() -> groupKeyWithoutExt.copy()))
						.map(us -> {
							T deserializedSuffix;
							try (var splittedGroupSuffix = splitGroupSuffix(groupKeyWithoutExt)) {
								deserializedSuffix = this.deserializeSuffix(splittedGroupSuffix);
								return new SubStageEntry<>(deserializedSuffix, us);
							}
						})
						.doFinally(s -> groupKeyWithoutExt.close())
				);
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
				.getAllValues(null, false)
				.concatWith(this
						.clear()
						.then(this.putMulti(entries))
						.then(Mono.empty())
				);
	}

	@Override
	public Mono<Void> clear() {
		return Mono.using(() -> rangeSupplier.get(), range -> {
			if (range.isAll()) {
				return dictionary.clear();
			} else if (range.isSingle()) {
				return dictionary
						.remove(Mono.fromCallable(() -> range.getSingleUnsafe()), LLDictionaryResultType.VOID)
						.doOnNext(resource -> LLUtils.finalizeResourceNow(resource))
						.then();
			} else {
				return dictionary.setRange(rangeMono, Flux.empty(), false);
			}
		}, resource -> LLUtils.finalizeResourceNow(resource));
	}

	protected T deserializeSuffix(@NotNull Buffer keySuffix) throws SerializationException {
		assert suffixKeyLengthConsistency(keySuffix.readableBytes());
		var result = keySuffixSerializer.deserialize(keySuffix);
		return result;
	}

	protected void serializeSuffix(T keySuffix, Buffer output) throws SerializationException {
		output.ensureWritable(keySuffixLength);
		var beforeWriterOffset = output.writerOffset();
		keySuffixSerializer.serialize(keySuffix, output);
		var afterWriterOffset = output.writerOffset();
		assert suffixKeyLengthConsistency(afterWriterOffset - beforeWriterOffset)
				: "Invalid key suffix length: " + (afterWriterOffset - beforeWriterOffset) + ". Expected: " + keySuffixLength;
	}

	public static <K1, K2, V, R> Flux<R> getAllLeaves2(DatabaseMapDictionaryDeep<K1, Object2ObjectSortedMap<K2, V>, ? extends DatabaseStageMap<K2, V, DatabaseStageEntry<V>>> deepMap,
			CompositeSnapshot snapshot,
			TriFunction<K1, K2, V, R> merger,
			@NotNull Mono<K1> savedProgressKey1) {
		var keySuffix1Serializer = deepMap.keySuffixSerializer;
		SerializerFixedBinaryLength<?> keySuffix2Serializer;
		Serializer<?> valueSerializer;
		boolean isHashed;
		boolean isHashedSet;
		if (deepMap.subStageGetter instanceof SubStageGetterMap subStageGetterMap) {
			isHashed = false;
			isHashedSet = false;
			keySuffix2Serializer = subStageGetterMap.keySerializer;
			valueSerializer = subStageGetterMap.valueSerializer;
		} else if (deepMap.subStageGetter instanceof SubStageGetterHashMap subStageGetterHashMap) {
			isHashed = true;
			isHashedSet = false;
			keySuffix2Serializer = subStageGetterHashMap.keyHashSerializer;

			//noinspection unchecked
			ValueWithHashSerializer<K2, V> valueWithHashSerializer = new ValueWithHashSerializer<>(
					(Serializer<K2>) subStageGetterHashMap.keySerializer,
					(Serializer<V>) subStageGetterHashMap.valueSerializer
			);
			valueSerializer = new ValuesSetSerializer<>(valueWithHashSerializer);
		} else if (deepMap.subStageGetter instanceof SubStageGetterHashSet subStageGetterHashSet) {
			isHashed = true;
			isHashedSet = true;
			keySuffix2Serializer = subStageGetterHashSet.keyHashSerializer;

			//noinspection unchecked
			valueSerializer = new ValuesSetSerializer<K2>(subStageGetterHashSet.keySerializer);
		} else {
			throw new IllegalArgumentException();
		}

		var savedProgressKey1Opt = savedProgressKey1.map(value1 -> Optional.of(value1)).defaultIfEmpty(Optional.empty());

		return deepMap
				.dictionary
				.getRange(deepMap.resolveSnapshot(snapshot), Mono.zip(savedProgressKey1Opt, deepMap.rangeMono).handle((tuple, sink) -> {
					var firstKey = tuple.getT1();
					var fullRange = tuple.getT2();
					try {
						if (firstKey.isPresent()) {
							try (var key1Buf = deepMap.alloc.allocate(keySuffix1Serializer.getSerializedBinaryLength())) {
								keySuffix1Serializer.serialize(firstKey.get(), key1Buf);
								sink.next(LLRange.of(key1Buf.send(), fullRange.getMax()));
							} catch (SerializationException e) {
								sink.error(e);
							}
						} else {
							sink.next(fullRange);
						}
					} catch (Throwable ex) {
						try {
							fullRange.close();
						} catch (Throwable ex2) {
							LOG.error(ex2);
						}
						throw ex;
					}
				}), false, false)
				.concatMapIterable(entry -> {
					K1 key1 = null;
					Object key2 = null;
					try (entry) {
						var keyBuf = entry.getKeyUnsafe();
						var valueBuf = entry.getValueUnsafe();
						try {
							assert keyBuf != null;
							keyBuf.skipReadableBytes(deepMap.keyPrefixLength);
							try (var key1Buf = keyBuf.split(deepMap.keySuffixLength)) {
								key1 = keySuffix1Serializer.deserialize(key1Buf);
							}
							key2 = keySuffix2Serializer.deserialize(keyBuf);
							assert valueBuf != null;
							Object value = valueSerializer.deserialize(valueBuf);
							if (isHashedSet) {
								//noinspection unchecked
								Set<K2> set = (Set<K2>) value;
								K1 finalKey1 = key1;
								//noinspection unchecked
								return set.stream().map(e -> merger.apply(finalKey1, e, (V) Nothing.INSTANCE)).toList();
							} else if (isHashed) {
								//noinspection unchecked
								Set<Entry<K2, V>> set = (Set<Entry<K2, V>>) value;
								K1 finalKey1 = key1;
								return set.stream().map(e -> merger.apply(finalKey1, e.getKey(), e.getValue())).toList();
							} else {
								//noinspection unchecked
								return List.of(merger.apply(key1, (K2) key2, (V) value));
							}
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
								return List.of();
							} else {
								throw ex;
							}
						}
					} catch (SerializationException ex) {
						throw new CompletionException(ex);
					}
				});
	}

	@Override
	protected void onClose() {
		try {
			if (rangeSupplier != null) {
				rangeSupplier.close();
			}
		} catch (Throwable ex) {
			LOG.error("Failed to close range", ex);
		}
		try {
			if (keyPrefixSupplier != null) {
				keyPrefixSupplier.close();
			}
		} catch (Throwable ex) {
			LOG.error("Failed to close keyPrefix", ex);
		}
	}
}
