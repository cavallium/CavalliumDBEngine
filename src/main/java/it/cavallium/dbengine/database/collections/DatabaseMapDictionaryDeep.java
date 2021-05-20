package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.disk.LLLocalDictionary;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

// todo: implement optimized methods
public class DatabaseMapDictionaryDeep<T, U, US extends DatabaseStage<U>> implements DatabaseStageMap<T, U, US> {

	protected final LLDictionary dictionary;
	private final ByteBufAllocator alloc;
	protected final SubStageGetter<U, US> subStageGetter;
	protected final SerializerFixedBinaryLength<T, ByteBuf> keySuffixSerializer;
	protected final ByteBuf keyPrefix;
	protected final int keyPrefixLength;
	protected final int keySuffixLength;
	protected final int keyExtLength;
	protected final LLRange range;
	private volatile boolean released;

	private static ByteBuf incrementPrefix(ByteBufAllocator alloc, ByteBuf originalKey, int prefixLength) {
		try {
			assert originalKey.readableBytes() >= prefixLength;
			ByteBuf copiedBuf = alloc.buffer(originalKey.writerIndex(), originalKey.writerIndex() + 1);
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
			ByteBuf zeroSuffixAndExt = alloc.buffer(suffixLength + extLength, suffixLength + extLength);
			try {
				zeroSuffixAndExt.writeZero(suffixLength + extLength);
				ByteBuf result = LLUtils.compositeBuffer(alloc, prefixKey.retain(), zeroSuffixAndExt.retain());
				try {
					return result.retain();
				} finally {
					result.release();
				}
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
		try {
			assert prefixKey.readableBytes() == prefixLength;
			assert suffixKey.readableBytes() == suffixLength;
			assert suffixLength > 0;
			assert extLength >= 0;
			ByteBuf result = LLUtils.compositeBuffer(alloc,
					prefixKey.retain(),
					suffixKey.retain(),
					alloc.buffer(extLength, extLength).writeZero(extLength)
			);
			try {
				assert result.readableBytes() == prefixLength + suffixLength + extLength;
				return result.retain();
			} finally {
				result.release();
			}
		} finally {
			prefixKey.release();
			suffixKey.release();
		}
	}

	/**
	 * Use DatabaseMapDictionaryRange.simple instead
	 */
	@Deprecated
	public static <T, U> DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, ByteBuf> keySerializer,
			SubStageGetterSingle<U> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary,
				dictionary.getAllocator().buffer(0),
				keySerializer,
				subStageGetter,
				0
		);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepTail(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, ByteBuf> keySerializer,
			int keyExtLength,
			SubStageGetter<U, US> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary,
				dictionary.getAllocator().buffer(0),
				keySerializer,
				subStageGetter,
				keyExtLength
		);
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
			assert prefixKey.refCnt() > 0;
			this.keyPrefix = prefixKey.retain();
			assert keyPrefix.refCnt() > 0;
			this.keyPrefixLength = keyPrefix.readableBytes();
			this.keySuffixLength = keySuffixSerializer.getSerializedBinaryLength();
			this.keyExtLength = keyExtLength;
			ByteBuf firstKey = firstRangeKey(alloc,
					keyPrefix.retain(),
					keyPrefixLength,
					keySuffixLength,
					keyExtLength
			);
			try {
				ByteBuf nextRangeKey = nextRangeKey(alloc,
						keyPrefix.retain(),
						keyPrefixLength,
						keySuffixLength,
						keyExtLength
				);
				try {
					assert keyPrefix.refCnt() > 0;
					assert keyPrefixLength == 0 || !LLUtils.equals(firstKey, nextRangeKey);
					this.range = keyPrefixLength == 0 ? LLRange.all() : LLRange.of(firstKey.retain(), nextRangeKey.retain());
					assert subStageKeysConsistency(keyPrefixLength + keySuffixLength + keyExtLength);
				} finally {
					nextRangeKey.release();
				}
			} finally {
				firstKey.release();
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
	protected ByteBuf stripPrefix(ByteBuf key, boolean slice) {
		try {
			if (slice) {
				return key.retainedSlice(this.keyPrefixLength, key.readableBytes() - this.keyPrefixLength);
			} else {
				return key.retain().readerIndex(key.readerIndex() + keyPrefixLength);
			}
		} finally {
			key.release();
		}
	}

	/**
	 * Remove ext from full key
	 */
	protected ByteBuf removeExtFromFullKey(ByteBuf key, boolean slice) {
		try {
			if (slice) {
				return key.retainedSlice(key.readerIndex(), keyPrefixLength + keySuffixLength);
			} else {
				return key.retain().writerIndex(key.writerIndex() - (keyPrefixLength + keySuffixLength));
			}
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
			ByteBuf result = LLUtils.compositeBuffer(alloc, keyPrefix.retain(), suffixKey.retain());
			assert keyPrefix.refCnt() > 0;
			try {
				assert result.readableBytes() == keyPrefixLength + keySuffixLength;
				return result.retain();
			} finally {
				result.release();
			}
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
			assert keyPrefix.refCnt() > 0;
			return LLRange.of(first, end);
		} finally {
			keySuffix.release();
		}
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return Mono
				.defer(() -> dictionary.sizeRange(resolveSnapshot(snapshot), range.retain(), fast))
				.doFirst(range::retain)
				.doAfterTerminate(range::release);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return Mono
				.defer(() -> dictionary.isRangeEmpty(resolveSnapshot(snapshot), range.retain()))
				.doFirst(range::retain)
				.doAfterTerminate(range::release);
	}

	@Override
	public Mono<US> at(@Nullable CompositeSnapshot snapshot, T keySuffix) {
		return Mono
				.using(
						() -> serializeSuffix(keySuffix),
						keySuffixData -> {
							Mono<List<ByteBuf>> debuggingKeysMono = Mono
									.defer(() -> {
										if (LLLocalDictionary.DEBUG_PREFIXES_WHEN_ASSERTIONS_ARE_ENABLED
												&& this.subStageGetter.needsDebuggingKeyFlux()) {
											return Flux
													.using(
															() -> toExtRange(keySuffixData.retain()),
															extRangeBuf -> this.dictionary
																	.getRangeKeys(resolveSnapshot(snapshot), extRangeBuf.retain()),
															LLRange::release
													)
													.collectList();
										} else {
											return Mono.just(List.of());
										}
									});
							return Mono
									.using(
											() -> toKeyWithoutExt(keySuffixData.retain()),
											keyBuf -> debuggingKeysMono
													.flatMap(debuggingKeysList -> this.subStageGetter
															.subStage(dictionary, snapshot, keyBuf.retain(), debuggingKeysList)
													),
											ReferenceCounted::release
									)
									.doOnDiscard(DatabaseStage.class, DatabaseStage::release);
						},
						ReferenceCounted::release
				)
				.doOnDiscard(DatabaseStage.class, DatabaseStage::release);
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return dictionary.getUpdateMode();
	}

	private static record GroupBuffers(ByteBuf groupKeyWithExt, ByteBuf groupKeyWithoutExt, ByteBuf groupSuffix) {}

	@Override
	public Flux<Entry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot) {
		return Flux
				.defer(() -> {
					if (LLLocalDictionary.DEBUG_PREFIXES_WHEN_ASSERTIONS_ARE_ENABLED && this.subStageGetter.needsDebuggingKeyFlux()) {
						return Flux
								.defer(() -> dictionary.getRangeKeysGrouped(resolveSnapshot(snapshot), range.retain(), keyPrefixLength + keySuffixLength))
								.flatMapSequential(rangeKeys -> Flux
										.using(
												() -> {
													assert this.subStageGetter.isMultiKey() || rangeKeys.size() == 1;
													ByteBuf groupKeyWithExt = rangeKeys.get(0).retainedSlice();
													ByteBuf groupKeyWithoutExt = removeExtFromFullKey(groupKeyWithExt.retain(), true);
													ByteBuf groupSuffix = this.stripPrefix(groupKeyWithoutExt.retain(), true);
													return new GroupBuffers(groupKeyWithExt, groupKeyWithoutExt, groupSuffix);
												},
												buffers -> Mono
														.fromCallable(() -> {
															assert subStageKeysConsistency(buffers.groupKeyWithExt.readableBytes());
															return null;
														})
														.then(this.subStageGetter
																.subStage(dictionary,
																		snapshot,
																		buffers.groupKeyWithoutExt.retain(),
																		rangeKeys.stream().map(ByteBuf::retain).collect(Collectors.toList())
																)
																.map(us -> Map.entry(this.deserializeSuffix(buffers.groupSuffix.retain()), us))
														),
												buffers -> {
													buffers.groupSuffix.release();
													buffers.groupKeyWithoutExt.release();
													buffers.groupKeyWithExt.release();
												}
										)
										.doAfterTerminate(() -> {
											for (ByteBuf rangeKey : rangeKeys) {
												rangeKey.release();
											}
										})
								)
								.doOnDiscard(Collection.class, discardedCollection -> {
									//noinspection unchecked
									var rangeKeys = (Collection<ByteBuf>) discardedCollection;
									for (ByteBuf rangeKey : rangeKeys) {
										rangeKey.release();
									}
								});
					} else {
						return Flux
								.defer(() -> dictionary.getRangeKeyPrefixes(resolveSnapshot(snapshot), range.retain(), keyPrefixLength + keySuffixLength))
								.flatMapSequential(groupKeyWithoutExt -> Mono
										.using(
												() -> {
													var groupSuffix = this.stripPrefix(groupKeyWithoutExt.retain(), true);
													assert subStageKeysConsistency(groupKeyWithoutExt.readableBytes() + keyExtLength);
													return groupSuffix;
												},
												groupSuffix -> this.subStageGetter
														.subStage(dictionary,
																snapshot,
																groupKeyWithoutExt.retain(),
																List.of()
														)
														.map(us -> Map.entry(this.deserializeSuffix(groupSuffix.retain()), us)),
												ReferenceCounted::release
										)
								);
					}
				})
				.doFirst(range::retain)
				.doAfterTerminate(range::release);
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
						.then(entries
								.flatMap(entry -> this
										.at(null, entry.getKey())
										.flatMap(us -> us
												.set(entry.getValue())
												.doAfterTerminate(us::release)
										)
								)
								.doOnDiscard(DatabaseStage.class, DatabaseStage::release)
								.then(Mono.empty())
						)
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
								.remove(range.getSingle().retain(), LLDictionaryResultType.VOID)
								.doOnNext(ReferenceCounted::release)
								.then();
					} else {
						return dictionary.setRange(range.retain(), Flux.empty());
					}
				});
	}

	//todo: temporary wrapper. convert the whole class to buffers
	protected T deserializeSuffix(ByteBuf keySuffix) {
		try {
			assert suffixKeyConsistency(keySuffix.readableBytes());
			var result = keySuffixSerializer.deserialize(keySuffix.retain());
			assert keyPrefix.refCnt() > 0;
			return result;
		} finally {
			keySuffix.release();
		}
	}

	//todo: temporary wrapper. convert the whole class to buffers
	protected ByteBuf serializeSuffix(T keySuffix) {
		ByteBuf suffixData = keySuffixSerializer.serialize(keySuffix);
		assert suffixKeyConsistency(suffixData.readableBytes());
		assert keyPrefix.refCnt() > 0;
		return suffixData;
	}

	@Override
	public void release() {
		if (!released) {
			released = true;
			this.range.release();
			this.keyPrefix.release();
		} else {
			throw new IllegalStateException("Already released");
		}
	}
}
