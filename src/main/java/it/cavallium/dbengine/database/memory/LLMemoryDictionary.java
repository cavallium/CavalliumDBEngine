package it.cavallium.dbengine.database.memory;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
		try (var b = buf.receive()) {
			return new BinaryLexicographicList(LLUtils.toArray(b));
		}
	}

	private Send<Buffer> kk(ByteList bytesList) {
		try (var buffer = getAllocator().allocate(bytesList.size())) {
			buffer.writeBytes(bytesList.toByteArray());
			return buffer.send();
		}
	}

	private Buffer kkB(ByteList bytesList) {
		var buffer = getAllocator().allocate(bytesList.size());
		try {
			buffer.writeBytes(bytesList.toByteArray());
			return buffer;
		} catch (Throwable t) {
			buffer.close();
			throw t;
		}
	}

	private BLRange r(Send<LLRange> send) {
		try(var range = send.receive()) {
			if (range.isAll()) {
				return new BLRange(null, null, null);
			} else if (range.isSingle()) {
				return new BLRange(null, null, k(range.getSingle()));
			} else if (range.hasMin() && range.hasMax()) {
				return new BLRange(k(range.getMin()), k(range.getMax()), null);
			} else if (range.hasMin()) {
				return new BLRange(k(range.getMin()), null, null);
			} else {
				return new BLRange(k(range.getMax()), null, null);
			}
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
		var kMono = keyMono.map(this::k);
		var vMono = valueMono.map(this::k);
		return Mono
				.zip(kMono, vMono)
				.mapNotNull(tuple -> mainDb.put(tuple.getT1(), tuple.getT2()))
				.transform(result -> this.transformResult(result, resultType))
				.onErrorMap(cause -> new IOException("Failed to read", cause));
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return Mono.just(updateMode);
	}

	@Override
	public Mono<Send<LLDelta>> updateAndGetDelta(Mono<Send<Buffer>> keyMono,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
			boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono,
				key -> Mono.fromCallable(() -> {
					try (key) {
						if (updateMode == UpdateMode.DISALLOW) {
							throw new UnsupportedOperationException("update() is disallowed");
						}
						AtomicReference<Send<Buffer>> oldRef = new AtomicReference<>(null);
						var newValue = mainDb.compute(k(key), (_unused, old) -> {
							if (old != null) {
								oldRef.set(kk(old));
							}
							Buffer v;
							try (var oldToSend = old != null ? kk(old) : null) {
								v = updater.apply(oldToSend);
							} catch (SerializationException e) {
								throw new IllegalStateException(e);
							}
							try {
								if (v != null) {
									return k(v.send());
								} else {
									return null;
								}
							} finally {
								if (v != null) {
									v.close();
								}
							}
						});
						return LLDelta.of(oldRef.get(), newValue != null ? kk(newValue) : null).send();
					}
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
	public Flux<Optional<Buffer>> getMulti(@Nullable LLSnapshot snapshot, Flux<Send<Buffer>> keys,
			boolean existsAlmostCertainly) {
		return keys.map(key -> {
			try (var t2 = key.receive()) {
				ByteList v = snapshots.get(resolveSnapshot(snapshot)).get(k(t2.copy().send()));
				if (v != null) {
					return Optional.of(kkB(v));
				} else {
					return Optional.empty();
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
						var oldValue = mainDb.put(k(key.copy().send()), k(val.send()));
						if (oldValue != null && getOldValues) {
							sink.next(LLEntry.of(key.send(), kk(oldValue)).send());
						}
					}
				}
			}
		});
	}

	@Override
	public <K> Flux<Boolean> updateMulti(Flux<K> keys,
			Flux<Send<Buffer>> serializedKeys,
			KVSerializationFunction<K, @Nullable Send<Buffer>, @Nullable Buffer> updateFunction) {
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
							if (element != null) {
								return LLEntry.of(single.send(), kk(element)).send();
							} else {
								return null;
							}
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
		return Flux.usingWhen(rangeMono, rangeToReceive -> {
			try (var range = rangeToReceive.receive()) {
				if (range.isSingle()) {
					var singleToReceive = range.getSingle();
					return Mono.fromCallable(() -> {
						try (var single = singleToReceive.receive()) {
							var element = snapshots.get(resolveSnapshot(snapshot)).get(k(single.copy().send()));
							if (element != null) {
								return List.of(LLEntry.of(single.send(), kk(element)).send());
							} else {
								return List.<Send<LLEntry>>of();
							}
						}
					}).flux();
				} else {
					var rangeToReceive2 = range.send();
					return Mono
							.fromCallable(() -> mapSlice(snapshot, rangeToReceive2))
							.flatMapMany(map -> Flux.fromIterable(map.entrySet()))
							.groupBy(k -> k.getKey().subList(0, prefixLength))
							.flatMap(groupedFlux -> groupedFlux
									.map(entry -> LLEntry.of(kk(entry.getKey()), kk(entry.getValue())).send())
									.collectList()
							);
				}
			}
		}, range -> Mono.fromRunnable(range::close));
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

	@Override
	public Flux<List<Send<Buffer>>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			int prefixLength) {
		return Flux.usingWhen(rangeMono, rangeToReceive -> {
			try (var range = rangeToReceive.receive()) {
				if (range.isSingle()) {
					var singleToReceive = range.getSingle();
					return Mono.fromCallable(() -> {
						try (var single = singleToReceive.receive()) {
							var containsElement = snapshots.get(resolveSnapshot(snapshot)).containsKey(k(single.copy().send()));
							if (containsElement) {
								return List.of(single.send());
							} else {
								return List.<Send<Buffer>>of();
							}
						}
					}).flux();
				} else {
					var rangeToReceive2 = range.send();
					return Mono
							.fromCallable(() -> mapSlice(snapshot, rangeToReceive2))
							.flatMapMany(map -> Flux.fromIterable(map.entrySet()))
							.groupBy(k -> k.getKey().subList(0, prefixLength))
							.flatMap(groupedFlux -> groupedFlux
									.map(entry -> kk(entry.getKey()))
									.collectList()
							);
				}
			}
		}, range -> Mono.fromRunnable(range::close));
	}

	@SuppressWarnings("RedundantCast")
	@Override
	public Flux<Send<Buffer>> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			int prefixLength) {
		return Flux.usingWhen(rangeMono, rangeToReceive -> {
			try (var range = rangeToReceive.receive()) {
				if (range.isSingle()) {
					var singleToReceive = range.getSingle();
					return Mono.fromCallable(() -> {
						try (var single = singleToReceive.receive()) {
							var k = k(single.copy().send());
							var containsElement = snapshots.get(resolveSnapshot(snapshot)).containsKey(k);
							if (containsElement) {
								return kk(k.subList(0, prefixLength));
							} else {
								return null;
							}
						}
					}).flux();
				} else {
					var rangeToReceive2 = range.send();
					return Mono
							.fromCallable(() -> mapSlice(snapshot, rangeToReceive2))
							.flatMapMany(map -> Flux.fromIterable(map.entrySet()))
							.map(k -> (ByteList) k.getKey().subList(0, prefixLength))
							.distinctUntilChanged()
							.map(this::kk);
				}
			}
		}, range -> Mono.fromRunnable(range::close));
	}

	@Override
	public Flux<BadBlock> badBlocks(Mono<Send<LLRange>> rangeMono) {
		return Flux.empty();
	}

	@Override
	public Mono<Void> setRange(Mono<Send<LLRange>> rangeMono, Flux<Send<LLEntry>> entries) {
		return Mono.usingWhen(rangeMono, rangeToReceive -> {
			try (var range = rangeToReceive.receive()) {
				Mono<Void> clearMono;
				if (range.isSingle()) {
					var singleToReceive = range.getSingle();
					clearMono = Mono.fromRunnable(() -> {
						try (var single = singleToReceive.receive()) {
							var k = k(single.copy().send());
							mainDb.remove(k);
						}
					});
				} else {
					var rangeToReceive2 = range.copy().send();
					clearMono = Mono.fromRunnable(() -> mapSlice(null, rangeToReceive2).clear());
				}

				var r = r(range.copy().send());

				return clearMono
						.thenMany(entries)
						.doOnNext(entryToReceive -> {
							try (var entry = entryToReceive.receive()) {
								if (!isInsideRange(r, k(entry.getKey()))) {
									throw new IndexOutOfBoundsException("Trying to set a key outside the range!");
								}
								mainDb.put(k(entry.getKey()), k(entry.getValue()));
							}
						})
						.then();
			}
		}, range -> Mono.fromRunnable(range::close));
	}

	private boolean isInsideRange(BLRange range, ByteList key) {
		if (range.isAll()) {
			return true;
		} else if (range.isSingle()) {
			var single = range.getSingle();
			return Objects.equals(single, key);
		} else if (range.hasMin() && range.hasMax()) {
			var min = range.getMin();
			var max = range.getMax();
			return min.compareTo(key) <= 0 && max.compareTo(key) > 0;
		} else if (range.hasMin()) {
			var min = range.getMin();
			return min.compareTo(key) <= 0;
		} else {
			var max = range.getMax();
			return max.compareTo(key) > 0;
		}
	}

	@Override
	public Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono, boolean fillCache) {
		return getRangeKeys(snapshot, rangeMono)
				.map(buf -> {
					buf.receive().close();
					return true;
				})
				.count()
				.map(count -> count == 0);
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
		return getRange(snapshot, rangeMono)
				.take(1, true)
				.singleOrEmpty();
	}

	@Override
	public Mono<Send<Buffer>> getOneKey(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return getRangeKeys(snapshot, rangeMono)
				.take(1, true)
				.singleOrEmpty();
	}

	@Override
	public Mono<Send<LLEntry>> removeOne(Mono<Send<LLRange>> rangeMono) {
		return Mono.usingWhen(rangeMono, rangeToReceive -> {
			try (var range = rangeToReceive.receive()) {
				if (range.isSingle()) {
					var singleToReceive = range.getSingle();
					return Mono.fromCallable(() -> {
						try (var single = singleToReceive.receive()) {
							var element = mainDb.remove(k(single.copy().send()));
							if (element != null) {
								return LLEntry.of(single.send(), kk(element)).send();
							} else {
								return null;
							}
						}
					});
				} else {
					var rangeToReceive2 = range.send();
					return Mono
							.fromCallable(() -> mapSlice(null, rangeToReceive2))
							.mapNotNull(map -> {
								var it = map.entrySet().iterator();
								if (it.hasNext()) {
									var next = it.next();
									it.remove();
									return next;
								} else {
									return null;
								}
							})
							.map(entry -> LLEntry.of(kk(entry.getKey()), kk(entry.getValue())).send());
				}
			}
		}, range -> Mono.fromRunnable(range::close));
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}
}
