package it.cavallium.dbengine.database.memory;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Resource;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.OptionalBuf;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.disk.BinarySerializationFunction;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

	private Mono<Buffer> transformResult(Mono<ByteList> result, LLDictionaryResultType resultType) {
		if (resultType == LLDictionaryResultType.PREVIOUS_VALUE) {
			// Don't retain the result because it has been removed from the skip list
			return result.map(this::kkB);
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

	private ByteList kShr(Buffer buf) {
		return new BinaryLexicographicList(LLUtils.toArray(buf));
	}

	private ByteList kOwn(Buffer buf) {
		try (buf) {
			return new BinaryLexicographicList(LLUtils.toArray(buf));
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

	private ConcurrentNavigableMap<ByteList, ByteList> mapSlice(LLSnapshot snapshot, LLRange range) {
		if (range.isAll()) {
			return snapshots.get(resolveSnapshot(snapshot));
		} else if (range.isSingle()) {
			var key = k(range.getSingle());
			var value = snapshots
					.get(resolveSnapshot(snapshot))
					.get(key);
			if (value != null) {
				return new ConcurrentSkipListMap<>(Map.of(key, value));
			} else {
				return new ConcurrentSkipListMap<>(Map.of());
			}
		} else if (range.hasMin() && range.hasMax()) {
			var min = k(range.getMin());
			var max = k(range.getMax());
			if (min.compareTo(max) > 0) {
				return new ConcurrentSkipListMap<>(Map.of());
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
	public Mono<Buffer> get(@Nullable LLSnapshot snapshot, Mono<Buffer> keyMono) {
		return Mono.usingWhen(keyMono,
				key -> Mono
						.fromCallable(() -> snapshots.get(resolveSnapshot(snapshot)).get(kShr(key)))
						.map(this::kkB)
						.onErrorMap(cause -> new IOException("Failed to read", cause)),
				key -> Mono.fromRunnable(key::close)
		);
	}

	@Override
	public Mono<Buffer> put(Mono<Buffer> keyMono, Mono<Buffer> valueMono, LLDictionaryResultType resultType) {
		var kMono = keyMono.map(this::kOwn);
		var vMono = valueMono.map(this::kOwn);
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
	public Mono<LLDelta> updateAndGetDelta(Mono<Buffer> keyMono, BinarySerializationFunction updater) {
		return Mono.usingWhen(keyMono,
				key -> Mono.fromCallable(() -> {
					if (updateMode == UpdateMode.DISALLOW) {
						throw new UnsupportedOperationException("update() is disallowed");
					}
					AtomicReference<ByteList> oldRef = new AtomicReference<>(null);
					var newValue = mainDb.compute(kShr(key), (_unused, old) -> {
						if (old != null) {
							oldRef.set(old);
						}
						Buffer v;
						var oldToSend = old != null ? kkB(old) : null;
						try {
							assert oldToSend == null || oldToSend.isAccessible();
							v = updater.apply(oldToSend);
							assert v == null || v.isAccessible();
						} catch (SerializationException e) {
							throw new IllegalStateException(e);
						} finally {
							if (oldToSend != null && oldToSend.isAccessible()) {
								oldToSend.close();
							}
						}
						if (v != null) {
							return kOwn(v);
						} else {
							return null;
						}
					});
					var oldVal = oldRef.get();
					return LLDelta.of(oldVal != null ? kkB(oldRef.get()) : null, newValue != null ? kkB(newValue) : null);
				}),
				key -> Mono.fromRunnable(key::close)
		);
	}

	@Override
	public Mono<Void> clear() {
		return Mono.fromRunnable(mainDb::clear);
	}

	@Override
	public Mono<Buffer> remove(Mono<Buffer> keyMono, LLDictionaryResultType resultType) {
		return Mono.usingWhen(keyMono,
				key -> Mono
						.fromCallable(() -> mainDb.remove(kShr(key)))
						// Don't retain the result because it has been removed from the skip list
						.mapNotNull(bytesList -> switch (resultType) {
							case VOID -> null;
							case PREVIOUS_VALUE_EXISTENCE -> LLUtils.booleanToResponseByteBuffer(allocator, true);
							case PREVIOUS_VALUE -> kkB(bytesList);
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
	public Flux<OptionalBuf> getMulti(@Nullable LLSnapshot snapshot, Flux<Buffer> keys) {
		return keys.map(key -> {
			try (key) {
				ByteList v = snapshots.get(resolveSnapshot(snapshot)).get(k(key.copy().send()));
				if (v != null) {
					return OptionalBuf.of(kkB(v));
				} else {
					return OptionalBuf.empty();
				}
			}
		});
	}

	@Override
	public Mono<Void> putMulti(Flux<LLEntry> entries) {
		return entries.doOnNext(entry -> {
			try (entry) {
				mainDb.put(k(entry.getKeyUnsafe().send()), k(entry.getValueUnsafe().send()));
			}
		}).then();
	}

	@Override
	public <K> Flux<Boolean> updateMulti(Flux<K> keys,
			Flux<Buffer> serializedKeys,
			KVSerializationFunction<K, @Nullable Buffer, @Nullable Buffer> updateFunction) {
		return Flux.error(new UnsupportedOperationException("Not implemented"));
	}

	@Override
	public Flux<LLEntry> getRange(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			boolean reverse,
			boolean smallRange) {
		return Flux.usingWhen(rangeMono, range -> {
			if (range.isSingle()) {
				var singleToReceive = range.getSingle();
				return Mono.fromCallable(() -> {
					try (var single = singleToReceive.receive()) {
						var element = snapshots.get(resolveSnapshot(snapshot)).get(k(single.copy().send()));
						if (element != null) {
							return LLEntry.of(single, kkB(element));
						} else {
							return null;
						}
					}
				}).flux();
			} else {
				return Mono
						.fromCallable(() -> mapSlice(snapshot, range))
						.flatMapIterable(map -> {
							if (reverse) {
								return map.descendingMap().entrySet();
							} else {
								return map.entrySet();
							}
						})
						.map(entry -> LLEntry.of(kkB(entry.getKey()), kkB(entry.getValue())));
			}
		}, range -> Mono.fromRunnable(range::close));
	}

	@Override
	public Flux<List<LLEntry>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			int prefixLength, boolean smallRange) {
		return Flux.usingWhen(rangeMono, range -> {
			try (range) {
				if (range.isSingle()) {
					var singleToReceive = range.getSingle();
					return Mono.fromCallable(() -> {
						try (var single = singleToReceive.receive()) {
							var element = snapshots.get(resolveSnapshot(snapshot)).get(k(single.copy().send()));
							if (element != null) {
								return List.of(LLEntry.of(single, kkB(element)));
							} else {
								return List.<LLEntry>of();
							}
						}
					}).flux();
				} else {
					return Mono
							.fromCallable(() -> mapSlice(snapshot, range))
							.flatMapIterable(SortedMap::entrySet)
							.groupBy(k -> k.getKey().subList(0, prefixLength))
							.flatMap(groupedFlux -> groupedFlux
									.map(entry -> LLEntry.of(kkB(entry.getKey()), kkB(entry.getValue())))
									.collectList()
							);
				}
			}
		}, range -> Mono.fromRunnable(range::close));
	}

	@Override
	public Flux<Buffer> getRangeKeys(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			boolean reverse,
			boolean smallRange) {
		return Flux.usingWhen(rangeMono,
				range -> {
					if (range.isSingle()) {
						var singleToReceive = range.getSingle();
						return Mono.fromCallable(() -> {
							var single = singleToReceive.receive();
							try {
								var contains = snapshots.get(resolveSnapshot(snapshot)).containsKey(k(single.copy().send()));
								return contains ? single : null;
							} catch (Throwable ex) {
								single.close();
								throw ex;
							}
						}).flux();
					} else {
						return Mono
								.fromCallable(() -> mapSlice(snapshot, range))
								.<ByteList>flatMapIterable(map -> {
									if (reverse) {
										return map.descendingMap().keySet();
									} else {
										return map.keySet();
									}
								})
								.map(this::kkB);
					}
				},
				range -> Mono.fromRunnable(range::close)
		);
	}

	@Override
	public Flux<List<Buffer>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			int prefixLength, boolean smallRange) {
		return Flux.usingWhen(rangeMono, range -> {
			try (range) {
				if (range.isSingle()) {
					var singleToReceive = range.getSingle();
					return Mono.fromCallable(() -> {
						var single = singleToReceive.receive();
						try {
							var containsElement = snapshots.get(resolveSnapshot(snapshot)).containsKey(k(single.copy().send()));
							if (containsElement) {
								return List.of(single);
							} else {
								return List.<Buffer>of();
							}
						} catch (Throwable ex) {
							single.close();
							throw ex;
						}
					}).flux();
				} else {
					return Mono
							.fromCallable(() -> mapSlice(snapshot, range))
							.flatMapIterable(SortedMap::entrySet)
							.groupBy(k -> k.getKey().subList(0, prefixLength))
							.flatMap(groupedFlux -> groupedFlux
									.map(entry -> kkB(entry.getKey()))
									.collectList()
							);
				}
			}
		}, range -> Mono.fromRunnable(range::close));
	}

	@SuppressWarnings("RedundantCast")
	@Override
	public Flux<Buffer> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			int prefixLength, boolean smallRange) {
		return Flux.usingWhen(rangeMono, range -> {
			try (range) {
				if (range.isSingle()) {
					var singleToReceive = range.getSingle();
					return Mono.fromCallable(() -> {
						try (var single = singleToReceive.receive()) {
							var k = k(single.copy().send());
							var containsElement = snapshots.get(resolveSnapshot(snapshot)).containsKey(k);
							if (containsElement) {
								return kkB(k.subList(0, prefixLength));
							} else {
								return null;
							}
						}
					}).flux();
				} else {
					return Mono
							.fromCallable(() -> mapSlice(snapshot, range))
							.flatMapIterable(SortedMap::entrySet)
							.map(k -> (ByteList) k.getKey().subList(0, prefixLength))
							.distinctUntilChanged()
							.map(this::kkB);
				}
			}
		}, range -> Mono.fromRunnable(range::close));
	}

	@Override
	public Flux<BadBlock> badBlocks(Mono<LLRange> rangeMono) {
		return Flux.empty();
	}

	@Override
	public Mono<Void> setRange(Mono<LLRange> rangeMono, Flux<LLEntry> entries, boolean smallRange) {
		return Mono.usingWhen(rangeMono, range -> {
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
				clearMono = Mono.fromRunnable(() -> mapSlice(null, range).clear());
			}

			var r = r(range.copy().send());

			return clearMono
					.thenMany(entries)
					.doOnNext(entry -> {
						try (entry) {
							if (!isInsideRange(r, kShr(entry.getKeyUnsafe()))) {
								throw new IndexOutOfBoundsException("Trying to set a key outside the range!");
							}
							mainDb.put(kShr(entry.getKeyUnsafe()), kShr(entry.getValueUnsafe()));
						}
					})
					.then();
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
	public Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono, boolean fillCache) {
		return getRangeKeys(snapshot, rangeMono, false, false)
				.doOnNext(Resource::close)
				.count()
				.map(count -> count == 0);
	}

	@Override
	public Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono, boolean fast) {
		return Mono.usingWhen(rangeMono,
				range -> Mono.fromCallable(() -> (long) mapSlice(snapshot, range).size()),
				range -> Mono.fromRunnable(range::close)
		);
	}

	@Override
	public Mono<LLEntry> getOne(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return getRange(snapshot, rangeMono, false, false)
				.take(1, true)
				.singleOrEmpty();
	}

	@Override
	public Mono<Buffer> getOneKey(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return getRangeKeys(snapshot, rangeMono, false, false)
				.take(1, true)
				.singleOrEmpty();
	}

	@Override
	public Mono<LLEntry> removeOne(Mono<LLRange> rangeMono) {
		return Mono.usingWhen(rangeMono, range -> {
			try (range) {
				if (range.isSingle()) {
					var singleToReceive = range.getSingle();
					return Mono.fromCallable(() -> {
						try (var single = singleToReceive.receive()) {
							var element = mainDb.remove(k(single.copy().send()));
							if (element != null) {
								return LLEntry.of(single, kkB(element));
							} else {
								return null;
							}
						}
					});
				} else {
					return Mono
							.fromCallable(() -> mapSlice(null, range))
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
							.map(entry -> LLEntry.of(kkB(entry.getKey()), kkB(entry.getValue())));
				}
			}
		}, range -> Mono.fromRunnable(range::close));
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}
}
