package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Resource;
import io.netty.buffer.api.Send;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.disk.LLLocalDictionary;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

// todo: implement optimized methods (which?)
public class DatabaseMapDictionaryDeep<T, U, US extends DatabaseStage<U>> implements DatabaseStageMap<T, U, US> {

	protected final LLDictionary dictionary;
	private final BufferAllocator alloc;
	protected final SubStageGetter<U, US> subStageGetter;
	protected final SerializerFixedBinaryLength<T, Send<Buffer>> keySuffixSerializer;
	protected final Buffer keyPrefix;
	protected final int keyPrefixLength;
	protected final int keySuffixLength;
	protected final int keyExtLength;
	protected final LLRange range;
	protected final Mono<Send<LLRange>> rangeMono;
	private volatile boolean released;

	private static Send<Buffer> incrementPrefix(BufferAllocator alloc, Send<Buffer> originalKeySend, int prefixLength) {
		try (var originalKey = originalKeySend.receive()) {
			assert originalKey.readableBytes() >= prefixLength;
			try (Buffer copiedBuf = alloc.allocate(originalKey.writerOffset())) {
				boolean overflowed = true;
				final int ff = 0xFF;
				int writtenBytes = 0;
				copiedBuf.writerOffset(prefixLength);
				for (int i = prefixLength - 1; i >= 0; i--) {
					int iByte = originalKey.getUnsignedByte(i);
					if (iByte != ff) {
						copiedBuf.setUnsignedByte(i, iByte + 1);
						writtenBytes++;
						overflowed = false;
						break;
					} else {
						copiedBuf.setUnsignedByte(i, 0x00);
						writtenBytes++;
						overflowed = true;
					}
				}
				assert prefixLength - writtenBytes >= 0;
				if (prefixLength - writtenBytes > 0) {
					originalKey.copyInto(0, copiedBuf, 0, (prefixLength - writtenBytes));
				}

				copiedBuf.writerOffset(copiedBuf.capacity());

				if (originalKey.writerOffset() - prefixLength > 0) {
					originalKey.copyInto(prefixLength, copiedBuf, prefixLength, originalKey.writerOffset() - prefixLength);
				}

				if (overflowed) {
					for (int i = 0; i < copiedBuf.writerOffset(); i++) {
						copiedBuf.setUnsignedByte(i, 0xFF);
					}
					copiedBuf.writeByte((byte) 0x00);
				}
				return copiedBuf.send();
			}
		}
	}

	static Send<Buffer> firstRangeKey(BufferAllocator alloc,
			Send<Buffer> prefixKey,
			int prefixLength,
			int suffixLength,
			int extLength) {
		return zeroFillKeySuffixAndExt(alloc, prefixKey, prefixLength, suffixLength, extLength);
	}

	static Send<Buffer> nextRangeKey(BufferAllocator alloc,
			Send<Buffer> prefixKey,
			int prefixLength,
			int suffixLength,
			int extLength) {
		try (prefixKey) {
			try (Send<Buffer> nonIncremented = zeroFillKeySuffixAndExt(alloc, prefixKey, prefixLength, suffixLength,
					extLength)) {
				return incrementPrefix(alloc, nonIncremented, prefixLength);
			}
		}
	}

	protected static Send<Buffer> zeroFillKeySuffixAndExt(BufferAllocator alloc,
			Send<Buffer> prefixKeySend,
			int prefixLength,
			int suffixLength,
			int extLength) {
		try (var prefixKey = prefixKeySend.receive()) {
			assert prefixKey.readableBytes() == prefixLength;
			assert suffixLength > 0;
			assert extLength >= 0;
			try (Buffer zeroSuffixAndExt = alloc.allocate(suffixLength + extLength)) {
				for (int i = 0; i < suffixLength + extLength; i++) {
					zeroSuffixAndExt.writeByte((byte) 0x0);
				}
				try (Send<Buffer> result = LLUtils.compositeBuffer(alloc, prefixKey.send(), zeroSuffixAndExt.send())) {
					return result;
				}
			}
		}
	}

	static Send<Buffer> firstRangeKey(
			BufferAllocator alloc,
			Send<Buffer> prefixKey,
			Send<Buffer> suffixKey,
			int prefixLength,
			int suffixLength,
			int extLength) {
		return zeroFillKeyExt(alloc, prefixKey, suffixKey, prefixLength, suffixLength, extLength);
	}

	static Send<Buffer> nextRangeKey(
			BufferAllocator alloc,
			Send<Buffer> prefixKey,
			Send<Buffer> suffixKey,
			int prefixLength,
			int suffixLength,
			int extLength) {
		try (Send<Buffer> nonIncremented = zeroFillKeyExt(alloc,
				prefixKey,
				suffixKey,
				prefixLength,
				suffixLength,
				extLength
		)) {
			return incrementPrefix(alloc, nonIncremented, prefixLength + suffixLength);
		}
	}

	protected static Send<Buffer> zeroFillKeyExt(
			BufferAllocator alloc,
			Send<Buffer> prefixKeySend,
			Send<Buffer> suffixKeySend,
			int prefixLength,
			int suffixLength,
			int extLength) {
		try (var prefixKey = prefixKeySend.receive()) {
			try (var suffixKey = suffixKeySend.receive()) {
				assert prefixKey.readableBytes() == prefixLength;
				assert suffixKey.readableBytes() == suffixLength;
				assert suffixLength > 0;
				assert extLength >= 0;

				try (var ext = alloc.allocate(extLength)) {
					for (int i = 0; i < extLength; i++) {
						ext.writeByte((byte) 0);
					}

					try (Buffer result = LLUtils.compositeBuffer(alloc, prefixKey.send(), suffixKey.send(), ext.send())
							.receive()) {
						assert result.readableBytes() == prefixLength + suffixLength + extLength;
						return result.send();
					}
				}
			}
		}
	}

	/**
	 * Use DatabaseMapDictionaryRange.simple instead
	 */
	@Deprecated
	public static <T, U> DatabaseMapDictionaryDeep<T, U, DatabaseStageEntry<U>> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, Send<Buffer>> keySerializer,
			SubStageGetterSingle<U> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary, dictionary.getAllocator().allocate(0).send(),
				keySerializer, subStageGetter, 0);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepTail(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, Send<Buffer>> keySerializer,
			int keyExtLength,
			SubStageGetter<U, US> subStageGetter) {
		return new DatabaseMapDictionaryDeep<>(dictionary,
				dictionary.getAllocator().allocate(0).send(),
				keySerializer,
				subStageGetter,
				keyExtLength
		);
	}

	public static <T, U, US extends DatabaseStage<U>> DatabaseMapDictionaryDeep<T, U, US> deepIntermediate(LLDictionary dictionary,
			Send<Buffer> prefixKey,
			SerializerFixedBinaryLength<T, Send<Buffer>> keySuffixSerializer,
			SubStageGetter<U, US> subStageGetter,
			int keyExtLength) {
		return new DatabaseMapDictionaryDeep<>(dictionary, prefixKey, keySuffixSerializer, subStageGetter, keyExtLength);
	}

	protected DatabaseMapDictionaryDeep(LLDictionary dictionary,
			Send<Buffer> prefixKey,
			SerializerFixedBinaryLength<T, Send<Buffer>> keySuffixSerializer,
			SubStageGetter<U, US> subStageGetter,
			int keyExtLength) {
		this.dictionary = dictionary;
		this.alloc = dictionary.getAllocator();
		this.subStageGetter = subStageGetter;
		this.keySuffixSerializer = keySuffixSerializer;
		this.keyPrefix = prefixKey.receive();
		assert keyPrefix.isAccessible();
		this.keyPrefixLength = keyPrefix.readableBytes();
		this.keySuffixLength = keySuffixSerializer.getSerializedBinaryLength();
		this.keyExtLength = keyExtLength;
		try (Buffer firstKey = firstRangeKey(alloc,
				keyPrefix.copy().send(),
				keyPrefixLength,
				keySuffixLength,
				keyExtLength
		).receive()) {
			try (Buffer nextRangeKey = nextRangeKey(alloc,
					keyPrefix.copy().send(),
					keyPrefixLength,
					keySuffixLength,
					keyExtLength
			).receive()) {
				assert keyPrefix.isAccessible();
				assert keyPrefixLength == 0 || !LLUtils.equals(firstKey, nextRangeKey);
				this.range = keyPrefixLength == 0 ? LLRange.all() : LLRange.of(firstKey.send(), nextRangeKey.send());
				this.rangeMono = LLUtils.lazyRetainRange(this.range);
				assert subStageKeysConsistency(keyPrefixLength + keySuffixLength + keyExtLength);
			}
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
	protected Send<Buffer> stripPrefix(Send<Buffer> keyToReceive) {
		try (var key = keyToReceive.receive()) {
			return key.copy(this.keyPrefixLength, key.readableBytes() - this.keyPrefixLength).send();
		}
	}

	/**
	 * Remove ext from full key
	 */
	protected Send<Buffer> removeExtFromFullKey(Send<Buffer> keyToReceive) {
		try (var key = keyToReceive.receive()) {
			return key.copy(key.readerOffset(), keyPrefixLength + keySuffixLength).send();
		}
	}

	/**
	 * Add prefix to suffix
	 */
	protected Send<Buffer> toKeyWithoutExt(Send<Buffer> suffixKeyToReceive) {
		try (var suffixKey = suffixKeyToReceive.receive()) {
			assert suffixKey.readableBytes() == keySuffixLength;
			try (Buffer result = LLUtils.compositeBuffer(alloc, keyPrefix.copy().send(), suffixKey.send()).receive()) {
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

	protected Send<LLRange> toExtRange(Buffer keySuffix) {
		try (Buffer first = firstRangeKey(alloc,
				keyPrefix.copy().send(),
				keySuffix.copy().send(),
				keyPrefixLength,
				keySuffixLength,
				keyExtLength
		).receive()) {
			try (Buffer end = nextRangeKey(alloc,
					keyPrefix.copy().send(),
					keySuffix.copy().send(),
					keyPrefixLength,
					keySuffixLength,
					keyExtLength
			).receive()) {
				return LLRange.of(first.send(), end.send()).send();
			}
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
		return Mono.using(
				() -> serializeSuffix(keySuffix).receive(),
				keySuffixData -> Mono.using(
						() -> toKeyWithoutExt(keySuffixData.send()).receive(),
						keyWithoutExt -> this.subStageGetter
								.subStage(dictionary, snapshot, LLUtils.lazyRetain(keyWithoutExt)),
						Resource::close
				),
				Resource::close
		).transform(LLUtils::handleDiscard).doOnDiscard(DatabaseStage.class, DatabaseStage::release);
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return dictionary.getUpdateMode();
	}

	@Override
	public Flux<BadBlock> badBlocks() {
		return dictionary.badBlocks(rangeMono);
	}

	private static record GroupBuffers(Buffer groupKeyWithExt, Buffer groupKeyWithoutExt, Buffer groupSuffix) {}

	@Override
	public Flux<Entry<T, US>> getAllStages(@Nullable CompositeSnapshot snapshot) {

		return Flux
				.defer(() -> dictionary.getRangeKeyPrefixes(resolveSnapshot(snapshot), rangeMono, keyPrefixLength + keySuffixLength))
				.flatMapSequential(groupKeyWithoutExtSend -> Mono
						.using(
								() -> {
									try (var groupKeyWithoutExt = groupKeyWithoutExtSend.receive()) {
										try (var groupSuffix = this.stripPrefix(groupKeyWithoutExt.copy().send()).receive()) {
											assert subStageKeysConsistency(groupKeyWithoutExt.readableBytes() + keyExtLength);
											return Tuples.of(groupKeyWithoutExt, groupSuffix);
										}
									}
								},
								groupKeyWithoutExtAndGroupSuffix -> this.subStageGetter
										.subStage(dictionary,
												snapshot,
												LLUtils.lazyRetain(groupKeyWithoutExtAndGroupSuffix.getT1())
										)
										.<Entry<T, US>>handle((us, sink) -> {
											try {
												sink.next(Map.entry(this.deserializeSuffix(groupKeyWithoutExtAndGroupSuffix.getT2().send()),
														us));
											} catch (SerializationException ex) {
												sink.error(ex);
											}
										}),
								entry -> {
									entry.getT1().close();
									entry.getT2().close();
								}
						)
				)
				.transform(LLUtils::handleDiscard);
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
								.remove(LLUtils.lazyRetain(range::getSingle), LLDictionaryResultType.VOID)
								.doOnNext(Send::close)
								.then();
					} else {
						return dictionary.setRange(LLUtils.lazyRetainRange(range), Flux.empty());
					}
				});
	}

	//todo: temporary wrapper. convert the whole class to buffers
	protected T deserializeSuffix(Send<Buffer> keySuffixToReceive) throws SerializationException {
		try (var keySuffix = keySuffixToReceive.receive()) {
			assert suffixKeyConsistency(keySuffix.readableBytes());
			var result = keySuffixSerializer.deserialize(keySuffix.send());
			assert keyPrefix.isAccessible();
			return result;
		}
	}

	//todo: temporary wrapper. convert the whole class to buffers
	protected Send<Buffer> serializeSuffix(T keySuffix) throws SerializationException {
		try (Buffer suffixData = keySuffixSerializer.serialize(keySuffix).receive()) {
			assert suffixKeyConsistency(suffixData.readableBytes());
			assert keyPrefix.isAccessible();
			return suffixData.send();
		}
	}

	@Override
	public void release() {
		if (!released) {
			released = true;
			this.range.close();
			this.keyPrefix.close();
		} else {
			throw new IllegalReferenceCountException(0, -1);
		}
	}
}
