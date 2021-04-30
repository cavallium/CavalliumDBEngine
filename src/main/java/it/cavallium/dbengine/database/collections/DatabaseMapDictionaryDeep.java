package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLLocalDictionary;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;
import static io.netty.buffer.Unpooled.*;

// todo: implement optimized methods
public class DatabaseMapDictionaryDeep<T, U, US extends DatabaseStage<U>> implements DatabaseStageMap<T, U, US> {

	public static final ByteBuf EMPTY_BYTES = unreleasableBuffer(directBuffer(0, 0));
	protected final LLDictionary dictionary;
	private final ByteBufAllocator alloc;
	protected final SubStageGetter<U, US> subStageGetter;
	protected final SerializerFixedBinaryLength<T, ByteBuf> keySuffixSerializer;
	protected final ByteBuf keyPrefix;
	protected final int keyPrefixLength;
	protected final int keySuffixLength;
	protected final int keyExtLength;
	protected final LLRange range;

	private static ByteBuf incrementPrefix(ByteBufAllocator alloc, ByteBuf originalKey, int prefixLength) {
		try {
			assert originalKey.readableBytes() >= prefixLength;
			ByteBuf copiedBuf = alloc.directBuffer(originalKey.writerIndex(), originalKey.writerIndex() + 1);
			try {
				boolean overflowed = true;
				final int ff = 0xFF;
				int writtenBytes = 0;
				copiedBuf.writerIndex(prefixLength);
				for (int i = prefixLength - 1; i >= 0; i--) {
					int iByte = originalKey.getUnsignedByte(i);
					if (iByte != ff) {
						copiedBuf.setByte(i, iByte + 1);
						writtenBytes++;
						overflowed = false;
						break;
					} else {
						copiedBuf.setByte(i, 0x00);
						writtenBytes++;
						overflowed = true;
					}
				}
				assert prefixLength - writtenBytes >= 0;
				if (prefixLength - writtenBytes > 0) {
					copiedBuf.setBytes(0, originalKey, 0, (prefixLength - writtenBytes));
				}

				copiedBuf.writerIndex(copiedBuf.capacity());

				if (originalKey.writerIndex() - prefixLength > 0) {
					copiedBuf.setBytes(prefixLength, originalKey, prefixLength, originalKey.writerIndex() - prefixLength);
				}

				if (overflowed) {
					for (int i = 0; i < copiedBuf.writerIndex(); i++) {
						copiedBuf.setByte(i, 0xFF);
					}
					copiedBuf.writeZero(1);
				}
				return copiedBuf.retain();
			} finally {
				copiedBuf.release();
			}
		} finally {
			originalKey.release();
		}
	}

	static ByteBuf firstRangeKey(ByteBufAllocator alloc, ByteBuf prefixKey, int prefixLength, int suffixLength, int extLength) {
		return zeroFillKeySuffixAndExt(alloc, prefixKey, prefixLength, suffixLength, extLength);
	}

	static ByteBuf nextRangeKey(ByteBufAllocator alloc, ByteBuf prefixKey, int prefixLength, int suffixLength, int extLength) {
		try {
			ByteBuf nonIncremented = zeroFillKeySuffixAndExt(alloc, prefixKey.retain(), prefixLength, suffixLength, extLength);
			try {
				return incrementPrefix(alloc, nonIncremented.retain(), prefixLength);
			} finally {
				nonIncremented.release();
			}
		} finally {
			prefixKey.release();
		}
	}

	protected static ByteBuf zeroFillKeySuffixAndExt(ByteBufAllocator alloc, ByteBuf prefixKey, int prefixLength, int suffixLength, int extLength) {
		try {
			assert prefixKey.readableBytes() == prefixLength;
			assert suffixLength > 0;
			assert extLength >= 0;
			if (!prefixKey.isDirect()) {
				throw new IllegalArgumentException("Prefix key must be a direct buffer");
			}
			assert prefixKey.nioBuffer().isDirect();
			ByteBuf zeroSuffixAndExt = alloc.directBuffer(suffixLength + extLength, suffixLength + extLength);
			try {
				assert zeroSuffixAndExt.isDirect();
				assert zeroSuffixAndExt.nioBuffer().isDirect();
				zeroSuffixAndExt.writeZero(suffixLength + extLength);
				ByteBuf result = LLUtils.directCompositeBuffer(alloc, prefixKey.retain(), zeroSuffixAndExt.retain());
				assert result.isDirect();
				assert result.nioBuffer().isDirect();
				return result;
			} finally {
				zeroSuffixAndExt.release();
			}
		} finally {
			prefixKey.release();
		}
	}

	static ByteBuf firstRangeKey(
			ByteBufAllocator alloc,
			ByteBuf prefixKey,
			ByteBuf suffixKey,
			int prefixLength,
			int suffixLength,
			int extLength) {
		return zeroFillKeyExt(alloc, prefixKey, suffixKey, prefixLength, suffixLength, extLength);
	}

	static ByteBuf nextRangeKey(
			ByteBufAllocator alloc,
			ByteBuf prefixKey,
			ByteBuf suffixKey,
			int prefixLength,
			int suffixLength,
			int extLength) {
		try {
			ByteBuf nonIncremented = zeroFillKeyExt(alloc,
					prefixKey.retain(),
					suffixKey.retain(),
					prefixLength,
					suffixLength,
					extLength
			);
			try {
				return incrementPrefix(alloc, nonIncremented.retain(), prefixLength + suffixLength);
			} finally {
				nonIncremented.release();
			}
		} finally {
			prefixKey.release();
			suffixKey.release();
		}
	}

	protected static ByteBuf zeroFillKeyExt(
			ByteBufAllocator alloc,
			ByteBuf prefixKey,
			ByteBuf suffixKey,
			int prefixLength,
			int suffixLength,
			int extLength) {
		assert prefixKey.readableBytes() == prefixLength;
		assert suffixKey.readableBytes() == suffixLength;
		assert suffixLength > 0;
		assert extLength >= 0;
		var result = LLUtils.directCompositeBuffer(alloc, prefixKey, suffixKey, alloc.buffer(extLength, extLength).writeZero(extLength));
		assert result.readableBytes() == prefixLength + suffixLength + extLength;
		return result;
	}

	/**
	 * Use DatabaseMapDictionaryRange.simple instead
	 */
	@Deprecated
	public static <T, U> DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, ByteBuf> keySerializer,
			SubStageGetterSingle<U> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary, EMPTY_BYTES, keySerializer, subStageGetter, 0);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepTail(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, ByteBuf> keySerializer,
			int keyExtLength,
			SubStageGetter<U, US> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary, EMPTY_BYTES, keySerializer, subStageGetter, keyExtLength);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepIntermediate(LLDictionary dictionary,
			ByteBuf prefixKey,
			SerializerFixedBinaryLength<T, ByteBuf> keySuffixSerializer,
			SubStageGetter<U, US> subStageGetter,
			int keyExtLength) {
		return new DatabaseMapDictionaryDeep<>(dictionary, prefixKey, keySuffixSerializer, subStageGetter, keyExtLength);
	}

	protected DatabaseMapDictionaryDeep(LLDictionary dictionary,
			ByteBuf prefixKey,
			SerializerFixedBinaryLength<T, ByteBuf> keySuffixSerializer,
			SubStageGetter<U, US> subStageGetter,
			int keyExtLength) {
		try {
			this.dictionary = dictionary;
			this.alloc = dictionary.getAllocator();
			this.subStageGetter = subStageGetter;
			this.keySuffixSerializer = keySuffixSerializer;
			this.keyPrefix = wrappedUnmodifiableBuffer(prefixKey).retain();
			this.keyPrefixLength = keyPrefix.readableBytes();
			this.keySuffixLength = keySuffixSerializer.getSerializedBinaryLength();
			this.keyExtLength = keyExtLength;
			if (!keyPrefix.isDirect()) {
				throw new IllegalArgumentException("KeyPrefix must be a direct buffer");
			}
			assert keyPrefix.isDirect();
			ByteBuf firstKey = wrappedUnmodifiableBuffer(firstRangeKey(alloc,
					keyPrefix.retain(),
					keyPrefixLength,
					keySuffixLength,
					keyExtLength
			));
			ByteBuf nextRangeKey = wrappedUnmodifiableBuffer(nextRangeKey(alloc,
					keyPrefix.retain(),
					keyPrefixLength,
					keySuffixLength,
					keyExtLength
			));
			try {
				assert keyPrefixLength == 0 || !LLUtils.equals(firstKey, nextRangeKey);
				assert firstKey.isDirect();
				assert nextRangeKey.isDirect();
				assert firstKey.nioBuffer().isDirect();
				assert nextRangeKey.nioBuffer().isDirect();
				this.range = keyPrefixLength == 0 ? LLRange.all() : LLRange.of(firstKey.retain(), nextRangeKey.retain());
				assert range == null || !range.hasMin() || range.getMin().isDirect();
				assert range == null || !range.hasMax() || range.getMax().isDirect();
				assert subStageKeysConsistency(keyPrefixLength + keySuffixLength + keyExtLength);
			} finally {
				firstKey.release();
				nextRangeKey.release();
			}
		} finally {
			prefixKey.release();
		}
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
	 * Keep only suffix and ext
	 */
	protected ByteBuf stripPrefix(ByteBuf key) {
		return key.slice(this.keyPrefixLength, key.readableBytes() - this.keyPrefixLength);
	}

	/**
	 * Remove ext from full key
	 */
	protected ByteBuf removeExtFromFullKey(ByteBuf key) {
		try {
			return key.slice(key.readerIndex(), keyPrefixLength + keySuffixLength).retain();
		} finally {
			key.release();
		}
	}

	/**
	 * Add prefix to suffix
	 */
	protected ByteBuf toKeyWithoutExt(ByteBuf suffixKey) {
		try {
			assert suffixKey.readableBytes() == keySuffixLength;
			ByteBuf result = LLUtils.directCompositeBuffer(alloc, keyPrefix.retain(), suffixKey.retain());
			assert result.readableBytes() == keyPrefixLength + keySuffixLength;
			return result;
		} finally {
			suffixKey.release();
		}
	}

	protected LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	protected LLRange toExtRange(ByteBuf keySuffix) {
		try {
			ByteBuf first = firstRangeKey(alloc,
					keyPrefix.retain(),
					keySuffix.retain(),
					keyPrefixLength,
					keySuffixLength,
					keyExtLength
			);
			ByteBuf end = nextRangeKey(alloc,
					keyPrefix.retain(),
					keySuffix.retain(),
					keyPrefixLength,
					keySuffixLength,
					keyExtLength
			);
			return LLRange.of(first, end);
		} finally {
			keySuffix.release();
		}
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary.sizeRange(resolveSnapshot(snapshot), range.retain(), fast);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary.isRangeEmpty(resolveSnapshot(snapshot), range.retain());
	}

	@Override
	public Mono<US> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		ByteBuf keySuffixData = serializeSuffix(keySuffix);
		Flux<ByteBuf> keyFlux;
		if (LLLocalDictionary.DEBUG_PREFIXES_WHEN_ASSERTIONS_ARE_ENABLED && this.subStageGetter.needsDebuggingKeyFlux()) {
			keyFlux = this.dictionary.getRangeKeys(resolveSnapshot(snapshot), toExtRange(keySuffixData.retain()));
		} else {
			keyFlux = Flux.empty();
		}
		return this.subStageGetter
				.subStage(dictionary, snapshot, toKeyWithoutExt(keySuffixData.retain()), keyFlux)
				.doFinally(s -> keySuffixData.release());
	}

	@Override
	public Flux<Entry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		if (LLLocalDictionary.DEBUG_PREFIXES_WHEN_ASSERTIONS_ARE_ENABLED && this.subStageGetter.needsDebuggingKeyFlux()) {
			return dictionary
					.getRangeKeysGrouped(resolveSnapshot(snapshot), range.retain(), keyPrefixLength + keySuffixLength)
					.flatMapSequential(rangeKeys -> {
						assert this.subStageGetter.isMultiKey() || rangeKeys.size() == 1;
						ByteBuf groupKeyWithExt = rangeKeys.get(0).retain();
						ByteBuf groupKeyWithoutExt = removeExtFromFullKey(groupKeyWithExt.retain());
						ByteBuf groupSuffix = this.stripPrefix(groupKeyWithoutExt.retain());
						assert subStageKeysConsistency(groupKeyWithExt.readableBytes());
						return this.subStageGetter
								.subStage(dictionary,
										snapshot,
										groupKeyWithoutExt,
										Flux.fromIterable(rangeKeys)
								)
								.map(us -> Map.entry(this.deserializeSuffix(wrappedUnmodifiableBuffer(groupSuffix.retain())), us))
								.doFinally(s -> {
									groupSuffix.release();
									groupKeyWithoutExt.release();
									groupKeyWithExt.release();
								});
					});
		} else {
			return dictionary
					.getRangeKeyPrefixes(resolveSnapshot(snapshot), range, keyPrefixLength + keySuffixLength)
					.flatMapSequential(groupKeyWithoutExt -> {
						ByteBuf groupSuffix = this.stripPrefix(groupKeyWithoutExt);
						assert subStageKeysConsistency(groupKeyWithoutExt.readableBytes() + keyExtLength);
						return this.subStageGetter
								.subStage(dictionary,
										snapshot,
										groupKeyWithoutExt,
										Flux.empty()
								)
								.map(us -> Map.entry(this.deserializeSuffix(wrappedUnmodifiableBuffer(groupSuffix)), us));
					});
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
		return getAllStages(null)
				.flatMapSequential(stage -> stage.getValue().get(null).map(val -> Map.entry(stage.getKey(), val)))
				.concatWith(clear().then(entries
						.flatMap(entry -> at(null, entry.getKey()).map(us -> Tuples.of(us, entry.getValue())))
						.flatMap(tuple -> tuple.getT1().set(tuple.getT2()))
						.then(Mono.empty())));
	}

	@Override
	public Mono<Void> clear() {
		if (range.isAll()) {
			return dictionary
					.clear();
		} else if (range.isSingle()) {
			return dictionary
					.remove(range.getSingle().retain(), LLDictionaryResultType.VOID)
					.then();
		} else {
			return dictionary
					.setRange(range.retain(), Flux.empty(), false)
					.then();
		}
	}

	//todo: temporary wrapper. convert the whole class to buffers
	protected T deserializeSuffix(ByteBuf keySuffix) {
		assert suffixKeyConsistency(keySuffix.readableBytes());
		return keySuffixSerializer.deserialize(keySuffix);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	protected ByteBuf serializeSuffix(T keySuffix) {
		ByteBuf suffixData = keySuffixSerializer.serialize(keySuffix);
		assert suffixKeyConsistency(suffixData.readableBytes());
		return suffixData;
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		range.release();
	}

	@Override
	public void release() {
		this.range.release();
		this.keyPrefix.release();
	}
}
