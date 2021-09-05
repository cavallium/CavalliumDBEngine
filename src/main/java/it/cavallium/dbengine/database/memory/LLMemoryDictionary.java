package it.cavallium.dbengine.database.memory;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.database.ExtraKeyOperationResult;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.serialization.BiSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

public class LLMemoryDictionary implements LLDictionary {

	private final String databaseName;
	private final String columnName;
	private final BufferAllocator allocator;
	private final UpdateMode updateMode;
	private final Getter<Long, ConcurrentSkipListMap<ByteList, ByteList>> snapshots;
	private final ConcurrentSkipListMap<ByteList, ByteList> mainDb;

	private interface Getter<T, U> {
		U get(T argument);
	}

	public LLMemoryDictionary(BufferAllocator allocator,
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
	public BufferAllocator getAllocator() {
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

	private Mono<Send<Buffer>> transformResult(Mono<ByteList> result, LLDictionaryResultType resultType) {
		if (resultType == LLDictionaryResultType.PREVIOUS_VALUE) {
			// Don't retain the result because it has been removed from the skip list
			return result.map(this::kk);
		} else if (resultType == LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE) {
			return result
					.map(prev -> true)
					.defaultIfEmpty(false)
					.map((Boolean bool) -> LLUtils.booleanToResponseByteBuffer(allocator, bool));
		} else {
			return result.then(Mono.empty());
		}
	}

	private ByteList k(Send<Buffer> buf) {
		return new BinaryLexicographicList(LLUtils.toArray(buf.receive()));
	}

	private Send<Buffer> kk(ByteList bytesList) {
		try (var buffer = getAllocator().allocate(bytesList.size())) {
			buffer.writeBytes(bytesList.toByteArray());
			return buffer.send();
		}
	}

	private Map<ByteList, ByteList> mapSlice(LLSnapshot snapshot, Send<LLRange> rangeToReceive) {
		try (var range = rangeToReceive.receive()) {
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
	}

	@Override
	public Mono<Send<Buffer>> get(@Nullable LLSnapshot snapshot, Mono<Send<Buffer>> keyMono, boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono,
				key -> Mono
						.fromCallable(() -> snapshots.get(resolveSnapshot(snapshot)).get(k(key)))
						.map(this::kk)
						.onErrorMap(cause -> new IOException("Failed to read", cause)),
				key -> Mono.fromRunnable(key::close)
		);
	}

	@Override
	public Mono<Send<Buffer>> put(Mono<Send<Buffer>> keyMono, Mono<Send<Buffer>> valueMono, LLDictionaryResultType resultType) {
		return Mono.usingWhen(keyMono,
				key -> Mono.usingWhen(valueMono,
						value -> Mono
								.fromCallable(() -> {
									var k = k(key);
									var v = k(value);
									return mainDb.put(k, v);
								})
								.transform(result -> this.transformResult(result, resultType))
								.onErrorMap(cause -> new IOException("Failed to read", cause)),
						value -> Mono.fromRunnable(value::close)
				),
				key -> Mono.fromRunnable(key::close)
		);
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return Mono.just(updateMode);
	}

	@Override
	public Mono<LLDelta> updateAndGetDelta(Mono<Send<Buffer>> keyMono,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Send<Buffer>> updater,
			boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono,
				key -> Mono.fromCallable(() -> {
					AtomicReference<Send<Buffer>> oldRef = new AtomicReference<>(null);
					var newValue = mainDb.compute(k(key), (_unused, old) -> {
						if (old != null) {
							oldRef.set(kk(old));
						}
						Send<Buffer> v = null;
						try {
							v = updater.apply(old != null ? kk(old) : null);
						} catch (SerializationException e) {
							throw new IllegalStateException(e);
						}
						try {
							return k(v);
						} finally {
							if (v != null) {
								v.close();
							}
						}
					});
					return LLDelta.of(oldRef.get(), kk(newValue));
				}),
				key -> Mono.fromRunnable(key::close)
		);
	}

	@Override
	public Mono<Void> clear() {
		return Mono.fromRunnable(mainDb::clear);
	}

	@Override
	public Mono<Send<Buffer>> remove(Mono<Send<Buffer>> keyMono, LLDictionaryResultType resultType) {
		return Mono.usingWhen(keyMono,
				key -> Mono
						.fromCallable(() -> mainDb.remove(k(key)))
						// Don't retain the result because it has been removed from the skip list
						.mapNotNull(bytesList -> switch (resultType) {
							case VOID -> null;
							case PREVIOUS_VALUE_EXISTENCE -> LLUtils.booleanToResponseByteBuffer(allocator, true);
							case PREVIOUS_VALUE -> kk(bytesList);
						})
						.switchIfEmpty(Mono.defer(() -> {
							if (resultType == LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE) {
								return Mono.fromCallable(() -> LLUtils.booleanToResponseByteBuffer(allocator, false));
							} else {
								return Mono.empty();
							}
						}))
						.onErrorMap(cause -> new IOException("Failed to read", cause)),
				key -> Mono.fromRunnable(key::close)
		);
	}

	@Override
	public <K> Flux<Tuple3<K, Send<Buffer>, Optional<Send<Buffer>>>> getMulti(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<K, Send<Buffer>>> keys,
			boolean existsAlmostCertainly) {
		return keys
				.map(key -> {
					try (var t2 = key.getT2().receive()) {
						ByteList v = snapshots.get(resolveSnapshot(snapshot)).get(k(t2.copy().send()));
						if (v != null) {
							return Tuples.of(key.getT1(), t2.send(), Optional.of(kk(v)));
						} else {
							return Tuples.of(key.getT1(), t2.send(), Optional.empty());
						}
					}
				});
	}

	@Override
	public Flux<Send<LLEntry>> putMulti(Flux<Send<LLEntry>> entries, boolean getOldValues) {
		return entries.handle((entryToReceive, sink) -> {
			try (var entry = entryToReceive.receive()) {
				try (var key = entry.getKey().receive()) {
					try (var val = entry.getValue().receive()) {
						var v = mainDb.put(k(key.copy().send()), k(val.send()));
						if (v == null || !getOldValues) {
							sink.complete();
						} else {
							sink.next(LLEntry.of(key.send(), kk(v)).send());
						}
					}
				}
			}
		});
	}

	@Override
	public <X> Flux<ExtraKeyOperationResult<Send<Buffer>, X>> updateMulti(Flux<Tuple2<Send<Buffer>, X>> entries,
			BiSerializationFunction<Send<Buffer>, X, Send<Buffer>> updateFunction) {
		return Flux.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Flux<Send<LLEntry>> getRange(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			boolean existsAlmostCertainly) {
		return Flux.usingWhen(rangeMono, rangeToReceive -> {
			try (var range = rangeToReceive.receive()) {
				if (range.isSingle()) {
					var singleToReceive = range.getSingle();
					return Mono.fromCallable(() -> {
						try (var single = singleToReceive.receive()) {
							var element = snapshots.get(resolveSnapshot(snapshot)).get(k(single.copy().send()));
							return LLEntry.of(single.send(), kk(element)).send();
						}
					}).flux();
				} else {
					var rangeToReceive2 = range.send();
					return Mono
							.fromCallable(() -> mapSlice(snapshot, rangeToReceive2))
							.flatMapMany(map -> Flux.fromIterable(map.entrySet()))
							.map(entry -> LLEntry.of(kk(entry.getKey()), kk(entry.getValue())).send());
				}
			}
		}, range -> Mono.fromRunnable(range::close));
	}

	@Override
	public Flux<List<Send<LLEntry>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			int prefixLength,
			boolean existsAlmostCertainly) {
		return Flux.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Flux<Send<Buffer>> getRangeKeys(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return Flux.usingWhen(rangeMono,
				rangeToReceive -> {
					try (var range = rangeToReceive.receive()) {
						if (range.isSingle()) {
							var singleToReceive = range.getSingle();
							return Mono.fromCallable(() -> {
								try (var single = singleToReceive.receive()) {
									var contains = snapshots.get(resolveSnapshot(snapshot)).containsKey(k(single.copy().send()));
									return contains ? single.send() : null;
								}
							}).flux();
						} else {
							var rangeToReceive2 = range.send();
							return Mono
									.fromCallable(() -> mapSlice(snapshot, rangeToReceive2))
									.flatMapMany(map -> Flux.fromIterable(map.entrySet()))
									.map(entry -> kk(entry.getKey()));
						}
					}
				},
				range -> Mono.fromRunnable(range::close)
		);
	}

	private static record BufferWithPrefix(Send<Buffer> buffer, Send<Buffer> prefix) {}

	@Override
	public Flux<List<Send<Buffer>>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			int prefixLength) {
		return getRangeKeys(snapshot, rangeMono)
				.map(bufferToReceive -> {
					try(var buffer = bufferToReceive.receive()) {
						try (var bufferPrefix = buffer.copy(buffer.readerOffset(), prefixLength)) {
							return new BufferWithPrefix(buffer.send(), bufferPrefix.send());
						}
					}
				})
				.windowUntilChanged(bufferTuple -> bufferTuple.prefix().receive(), LLUtils::equals)
				.flatMapSequential(window -> window.map(tuple -> {
					try (var ignored = tuple.prefix()) {
						return tuple.buffer();
					}
				}).collectList());
	}

	@Override
	public Flux<Send<Buffer>> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			int prefixLength) {
		return getRangeKeys(snapshot, rangeMono)
				.map(bufferToReceive -> {
					try(var buffer = bufferToReceive.receive()) {
						try (var bufferPrefix = buffer.copy(buffer.readerOffset(), prefixLength)) {
							return new BufferWithPrefix(buffer.send(), bufferPrefix.send());
						}
					}
				})
				.distinctUntilChanged(bufferTuple -> bufferTuple.prefix().receive(), (a, b) -> {
					if (LLUtils.equals(a, b)) {
						b.close();
						return true;
					} else {
						return false;
					}
				})
				.map(tuple -> {
					try (var ignored = tuple.prefix()) {
						return tuple.buffer();
					}
				})
				.transform(LLUtils::handleDiscard);
	}

	@Override
	public Flux<BadBlock> badBlocks(Mono<Send<LLRange>> rangeMono) {
		return Flux.empty();
	}

	@Override
	public Mono<Void> setRange(Mono<Send<LLRange>> rangeMono, Flux<Send<LLEntry>> entries) {
		return Mono.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return Mono.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono, boolean fast) {
		return Mono.usingWhen(rangeMono,
				range -> Mono.fromCallable(() -> (long) mapSlice(snapshot, range).size()),
				range -> Mono.fromRunnable(range::close)
		);
	}

	@Override
	public Mono<Send<LLEntry>> getOne(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return Mono.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Mono<Send<Buffer>> getOneKey(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return Mono.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Mono<Send<LLEntry>> removeOne(Mono<Send<LLRange>> rangeMono) {
		return Mono.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}
}
