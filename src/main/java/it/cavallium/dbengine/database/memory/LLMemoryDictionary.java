package it.cavallium.dbengine.database.memory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.ExtraKeyOperationResult;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

public class LLMemoryDictionary implements LLDictionary {

	private final String databaseName;
	private final String columnName;
	private final ByteBufAllocator allocator;
	private final UpdateMode updateMode;
	private final Getter<Long, ConcurrentSkipListMap<ByteList, ByteList>> snapshots;
	private final ConcurrentSkipListMap<ByteList, ByteList> mainDb;

	private interface Getter<T, U> {
		U get(T argument);
	}

	public LLMemoryDictionary(ByteBufAllocator allocator,
			String databaseName,
			String columnName,
			UpdateMode updateMode,
			ConcurrentHashMap<Long, ConcurrentHashMap<String, ConcurrentSkipListMap<ByteList, ByteList>>> snapshots,
			ConcurrentHashMap<String, ConcurrentSkipListMap<ByteList, ByteList>> mainDb) {
		this.databaseName = databaseName;
		this.columnName = columnName;
		this.allocator = allocator;
		this.updateMode = updateMode;
		this.snapshots = (snapshotId) -> snapshots.get(snapshotId).get(columnName);
		this.mainDb = mainDb.get(columnName);
	}

	@Override
	public String getColumnName() {
		return columnName;
	}

	@Override
	public ByteBufAllocator getAllocator() {
		return allocator;
	}

	private long resolveSnapshot(LLSnapshot snapshot) {
		if (snapshot == null) {
			return Long.MIN_VALUE + 1L;
		} else if (snapshot.getSequenceNumber() == Long.MIN_VALUE + 1L) {
			throw new IllegalStateException();
		} else {
			return snapshot.getSequenceNumber();
		}
	}

	private Mono<ByteBuf> transformResult(Mono<ByteList> result, LLDictionaryResultType resultType) {
		if (resultType == LLDictionaryResultType.PREVIOUS_VALUE) {
			// Don't retain the result because it has been removed from the skip list
			return result.map(this::kk);
		} else if (resultType == LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE) {
			return result
					.map(prev -> true)
					.defaultIfEmpty(false)
					.map(LLUtils::booleanToResponseByteBuffer);
		} else {
			return result.then(Mono.empty());
		}
	}

	private ByteList k(ByteBuf buf) {
		return new BinaryLexicographicList(LLUtils.toArray(buf));
	}

	private ByteBuf kk(ByteList bytesList) {
		var buffer = getAllocator().buffer(bytesList.size());
		buffer.writeBytes(bytesList.toByteArray());
		return buffer;
	}

	private Map<ByteList, ByteList> mapSlice(LLSnapshot snapshot, LLRange range) {
		if (range.isAll()) {
			return snapshots.get(resolveSnapshot(snapshot));
		} else if (range.isSingle()) {
			var key = k(range.getSingle());
			var value = snapshots
					.get(resolveSnapshot(snapshot))
					.get(key);
			if (value != null) {
				return Map.of(key, value);
			} else {
				return Map.of();
			}
		} else if (range.hasMin() && range.hasMax()) {
			var min = k(range.getMin());
			var max = k(range.getMax());
			if (min.compareTo(max) > 0) {
				return Map.of();
			}
			return snapshots
					.get(resolveSnapshot(snapshot))
					.subMap(min, true, max, false);
		} else if (range.hasMin()) {
			return snapshots
					.get(resolveSnapshot(snapshot))
					.tailMap(k(range.getMin()), true);
		} else {
			return snapshots
					.get(resolveSnapshot(snapshot))
					.headMap(k(range.getMax()), false);
		}
	}

	@Override
	public Mono<ByteBuf> get(@Nullable LLSnapshot snapshot, Mono<ByteBuf> keyMono, boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono,
				key -> Mono
						.fromCallable(() -> snapshots.get(resolveSnapshot(snapshot)).get(k(key)))
						.map(this::kk)
						.onErrorMap(cause -> new IOException("Failed to read " + LLUtils.toStringSafe(key), cause)),
				key -> Mono.fromRunnable(key::release)
		);
	}

	@Override
	public Mono<ByteBuf> put(Mono<ByteBuf> keyMono, Mono<ByteBuf> valueMono, LLDictionaryResultType resultType) {
		return Mono.usingWhen(keyMono,
				key -> Mono.usingWhen(valueMono,
						value -> Mono
								.fromCallable(() -> mainDb.put(k(key), k(value)))
								.transform(result -> this.transformResult(result, resultType))
								.onErrorMap(cause -> new IOException("Failed to read " + LLUtils.toStringSafe(key), cause)),
						value -> Mono.fromRunnable(value::release)
				),
				key -> Mono.fromRunnable(key::release)
		);
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return Mono.just(updateMode);
	}

	@Override
	public Mono<Delta<ByteBuf>> updateAndGetDelta(Mono<ByteBuf> keyMono,
			Function<@Nullable ByteBuf, @Nullable ByteBuf> updater,
			boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono,
				key -> Mono.fromCallable(() -> {
					AtomicReference<ByteBuf> oldRef = new AtomicReference<>(null);
					var newValue = mainDb.compute(k(key), (_unused, old) -> {
						if (old != null) {
							oldRef.set(kk(old));
						}
						var v = updater.apply(old != null ? kk(old) : null);
						try {
							return k(v);
						} finally {
							if (v != null) {
								v.release();
							}
						}
					});
					return new Delta<>(oldRef.get(), kk(newValue));
				}),
				key -> Mono.fromRunnable(key::release)
		);
	}

	@Override
	public Mono<Void> clear() {
		return Mono.fromRunnable(mainDb::clear);
	}

	@Override
	public Mono<ByteBuf> remove(Mono<ByteBuf> keyMono, LLDictionaryResultType resultType) {
		return Mono.usingWhen(keyMono,
				key -> Mono
						.fromCallable(() -> mainDb.remove(k(key)))
						// Don't retain the result because it has been removed from the skip list
						.mapNotNull(bytesList -> switch (resultType) {
							case VOID -> null;
							case PREVIOUS_VALUE_EXISTENCE -> LLUtils.booleanToResponseByteBuffer(true);
							case PREVIOUS_VALUE -> kk(bytesList);
						})
						.switchIfEmpty(Mono.defer(() -> {
							if (resultType == LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE) {
								return Mono.fromCallable(() -> LLUtils.booleanToResponseByteBuffer(false));
							} else {
								return Mono.empty();
							}
						}))
						.onErrorMap(cause -> new IOException("Failed to read " + LLUtils.toStringSafe(key), cause)),
				key -> Mono.fromRunnable(key::release)
		);
	}

	@Override
	public <K> Flux<Tuple3<K, ByteBuf, Optional<ByteBuf>>> getMulti(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<K, ByteBuf>> keys,
			boolean existsAlmostCertainly) {
		return keys
				.flatMapSequential(key -> {
					try {
						ByteList v = snapshots.get(resolveSnapshot(snapshot)).get(k(key.getT2()));
						if (v != null) {
							return Flux.just(Tuples.of(key.getT1(), key.getT2().retain(), Optional.of(kk(v))));
						} else {
							return Flux.just(Tuples.of(key.getT1(), key.getT2().retain(), Optional.empty()));
						}
					} finally {
						key.getT2().release();
					}
				});
	}

	@Override
	public Flux<Entry<ByteBuf, ByteBuf>> putMulti(Flux<Entry<ByteBuf, ByteBuf>> entries, boolean getOldValues) {
		return entries
				.handle((entry, sink) -> {
					var key = entry.getKey();
					var val = entry.getValue();
					try {
						var v = mainDb.put(k(key), k(val));
						if (v == null || !getOldValues) {
							sink.complete();
						} else {
							sink.next(Map.entry(key.retain(), kk(v)));
						}
					} finally {
						key.release();
						val.release();
					}
				});
	}

	@Override
	public <X> Flux<ExtraKeyOperationResult<ByteBuf, X>> updateMulti(Flux<Tuple2<ByteBuf, X>> entries,
			BiFunction<ByteBuf, X, ByteBuf> updateFunction) {
		return Flux.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Flux<Entry<ByteBuf, ByteBuf>> getRange(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			boolean existsAlmostCertainly) {
		return Flux.usingWhen(rangeMono,
				range -> {
					if (range.isSingle()) {
						return Mono.fromCallable(() -> {
							var element = snapshots.get(resolveSnapshot(snapshot))
									.get(k(range.getSingle()));
							return Map.entry(range.getSingle().retain(), kk(element));
						}).flux();
					} else {
						return Mono
								.fromCallable(() -> mapSlice(snapshot, range))
								.flatMapMany(map -> Flux.fromIterable(map.entrySet()))
								.map(entry -> Map.entry(kk(entry.getKey()), kk(entry.getValue())));
					}
				},
				range -> Mono.fromRunnable(range::release)
		);
	}

	@Override
	public Flux<List<Entry<ByteBuf, ByteBuf>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			int prefixLength,
			boolean existsAlmostCertainly) {
		return Flux.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Flux<ByteBuf> getRangeKeys(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return Flux.usingWhen(rangeMono,
				range -> {
					if (range.isSingle()) {
						return Mono.fromCallable(() -> {
							var contains = snapshots.get(resolveSnapshot(snapshot))
									.containsKey(k(range.getSingle()));
							return contains ? range.getSingle().retain() : null;
						}).flux();
					} else {
						return Mono
								.fromCallable(() -> mapSlice(snapshot, range))
								.flatMapMany(map -> Flux.fromIterable(map.entrySet()))
								.map(entry -> kk(entry.getKey()));
					}
				},
				range -> Mono.fromRunnable(range::release)
		);
	}

	@Override
	public Flux<List<ByteBuf>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			int prefixLength) {
		return getRangeKeys(snapshot, rangeMono)
				.bufferUntilChanged(k -> k.slice(k.readerIndex(), prefixLength), LLUtils::equals);
	}

	@Override
	public Flux<ByteBuf> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono, int prefixLength) {
		return getRangeKeys(snapshot, rangeMono)
				.distinctUntilChanged(k -> k.slice(k.readerIndex(), prefixLength), LLUtils::equals)
				.map(k -> k.slice(k.readerIndex(), prefixLength));
	}

	@Override
	public Flux<BadBlock> badBlocks(Mono<LLRange> rangeMono) {
		return Flux.empty();
	}

	@Override
	public Mono<Void> setRange(Mono<LLRange> rangeMono, Flux<Entry<ByteBuf, ByteBuf>> entries) {
		return Mono.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return Mono.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono, boolean fast) {
		return Mono.usingWhen(rangeMono,
				range -> Mono.fromCallable(() -> (long) mapSlice(snapshot, range).size()),
				range -> Mono.fromRunnable(range::release)
		);
	}

	@Override
	public Mono<Entry<ByteBuf, ByteBuf>> getOne(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return Mono.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Mono<ByteBuf> getOneKey(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return Mono.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Mono<Entry<ByteBuf, ByteBuf>> removeOne(Mono<LLRange> rangeMono) {
		return Mono.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}
}
