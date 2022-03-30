package it.cavallium.dbengine.database.disk;

import static io.netty5.buffer.api.StandardAllocationTypes.OFF_HEAP;
import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.fromByteArray;
import static it.cavallium.dbengine.database.LLUtils.isReadOnlyDirect;
import static it.cavallium.dbengine.database.LLUtils.toStringSafe;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.ReadableComponent;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.database.ColumnUtils;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractSlice;
import org.rocksdb.CappedWriteBatch;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.DirectSlice;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

public class LLLocalDictionary implements LLDictionary {

	protected static final Logger logger = LogManager.getLogger(LLLocalDictionary.class);
	private static final boolean USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS = false;
	static final int RESERVED_WRITE_BATCH_SIZE = 2 * 1024 * 1024; // 2MiB
	static final long MAX_WRITE_BATCH_SIZE = 1024L * 1024L * 1024L; // 1GiB
	static final int CAPPED_WRITE_BATCH_CAP = 50000; // 50K operations
	static final int MULTI_GET_WINDOW = 16;
	static final ReadOptions EMPTY_READ_OPTIONS = new UnreleasableReadOptions(new UnmodifiableReadOptions());
	static final WriteOptions EMPTY_WRITE_OPTIONS = new UnreleasableWriteOptions(new UnmodifiableWriteOptions());
	static final WriteOptions BATCH_WRITE_OPTIONS = new UnreleasableWriteOptions(new UnmodifiableWriteOptions());
	static final boolean PREFER_AUTO_SEEK_BOUND = false;
	/**
	 * It used to be false,
	 * now it's true to avoid crashes during iterations on completely corrupted files
	 */
	static final boolean VERIFY_CHECKSUMS_WHEN_NOT_NEEDED = false;
	/**
	 * Default: true. Use false to debug problems with windowing.
	 */
	static final boolean USE_WINDOW_IN_SET_RANGE = true;
	/**
	 * Default: true. Use false to debug problems with write batches.
	 */
	static final boolean USE_WRITE_BATCHES_IN_PUT_MULTI = true;
	/**
	 * Default: true. Use false to debug problems with write batches.
	 */
	static final boolean USE_WRITE_BATCHES_IN_SET_RANGE = true;
	/**
	 * Default: true. Use false to debug problems with capped write batches.
	 */
	static final boolean USE_CAPPED_WRITE_BATCH_IN_SET_RANGE = true;
	/**
	 * Default: true. Use false to debug problems with write batches deletes.
	 */
	static final boolean USE_WRITE_BATCH_IN_SET_RANGE_DELETE = false;
	static final boolean PARALLEL_EXACT_SIZE = true;

	private static final byte[] FIRST_KEY = new byte[]{};
	/**
	 * 1KiB dummy buffer, write only, used for debugging purposes
	 */
	private static final ByteBuffer DUMMY_WRITE_ONLY_BYTE_BUFFER = ByteBuffer.allocateDirect(1024);

	private final RocksDBColumn db;
	private final ColumnFamilyHandle cfh;
	private final String databaseName;
	private final String columnName;
	private final Scheduler dbScheduler;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;
	private final UpdateMode updateMode;
	private final boolean nettyDirect;
	private final BufferAllocator alloc;

	private final Counter startedUpdates;
	private final Counter endedUpdates;
	private final Timer updateTime;
	private final Counter startedGet;
	private final Counter endedGet;
	private final Timer getTime;
	private final Counter startedContains;
	private final Counter endedContains;
	private final Timer containsTime;
	private final Counter startedPut;
	private final Counter endedPut;
	private final Timer putTime;
	private final Counter startedRemove;
	private final Counter endedRemove;
	private final Timer removeTime;

	public LLLocalDictionary(
			BufferAllocator allocator,
			@NotNull RocksDBColumn db,
			String databaseName,
			String columnName,
			Scheduler dbScheduler,
			Function<LLSnapshot, Snapshot> snapshotResolver,
			UpdateMode updateMode,
			DatabaseOptions databaseOptions) {
		requireNonNull(db);
		this.db = db;
		this.cfh = db.getColumnFamilyHandle();
		this.databaseName = databaseName;
		this.columnName = columnName;
		this.dbScheduler = dbScheduler;
		this.snapshotResolver = snapshotResolver;
		this.updateMode = updateMode;
		alloc = allocator;
		this.nettyDirect = databaseOptions.allowNettyDirect() && alloc.getAllocationType() == OFF_HEAP;
		var meterRegistry = db.getMeterRegistry();

		this.startedGet = meterRegistry.counter("db.read.map.get.started.counter", "db.name", databaseName, "db.column", columnName);
		this.endedGet = meterRegistry.counter("db.read.map.get.ended.counter", "db.name", databaseName, "db.column", columnName);
		this.getTime = Timer
				.builder("db.read.map.get.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName)
				.register(meterRegistry);
		this.startedContains = meterRegistry.counter("db.read.map.contains.started.counter", "db.name", databaseName, "db.column", columnName);
		this.endedContains = meterRegistry.counter("db.read.map.contains.ended.counter", "db.name", databaseName, "db.column", columnName);
		this.containsTime = Timer
				.builder("db.read.map.contains.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName)
				.register(meterRegistry);
		this.startedUpdates = meterRegistry.counter("db.write.map.update.started.counter", "db.name", databaseName, "db.column", columnName);
		this.endedUpdates = meterRegistry.counter("db.write.map.update.ended.counter", "db.name", databaseName, "db.column", columnName);
		this.updateTime = Timer
				.builder("db.write.map.update.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName)
				.register(meterRegistry);
		this.startedPut = meterRegistry.counter("db.write.map.put.started.counter", "db.name", databaseName, "db.column", columnName);
		this.endedPut = meterRegistry.counter("db.write.map.put.ended.counter", "db.name", databaseName, "db.column", columnName);
		this.putTime = Timer
				.builder("db.write.map.put.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName)
				.register(meterRegistry);
		this.startedRemove = meterRegistry.counter("db.write.map.remove.started.counter", "db.name", databaseName, "db.column", columnName);
		this.endedRemove = meterRegistry.counter("db.write.map.remove.ended.counter", "db.name", databaseName, "db.column", columnName);
		this.removeTime = Timer
				.builder("db.write.map.remove.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName)
				.register(meterRegistry);
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}

	public String getColumnName() {
		return columnName;
	}

	/**
	 * Please don't modify the returned ReadOptions!
	 * If you want to modify it, wrap it into a new ReadOptions!
	 */
	private ReadOptions resolveSnapshot(LLSnapshot snapshot) {
		if (snapshot != null) {
			return getReadOptions(snapshotResolver.apply(snapshot));
		} else {
			return EMPTY_READ_OPTIONS;
		}
	}

	/**
	 * Please don't modify the returned ReadOptions!
	 * If you want to modify it, wrap it into a new ReadOptions!
	 */
	private ReadOptions getReadOptions(Snapshot snapshot) {
		if (snapshot != null) {
			return new ReadOptions().setSnapshot(snapshot);
		} else {
			return EMPTY_READ_OPTIONS;
		}
	}

	@Override
	public BufferAllocator getAllocator() {
		return alloc;
	}

	private <T> @NotNull Mono<T> runOnDb(Callable<@Nullable T> callable) {
		return Mono.fromCallable(callable).subscribeOn(dbScheduler);
	}

	@Override
	public Mono<Send<Buffer>> get(@Nullable LLSnapshot snapshot, Mono<Send<Buffer>> keyMono) {
		return keyMono
				.publishOn(dbScheduler)
				.<Send<Buffer>>handle((keySend, sink) -> {
					try (var key = keySend.receive()) {
						logger.trace(MARKER_ROCKSDB, "Reading {}", () -> toStringSafe(key));
						try {
							var readOptions = requireNonNullElse(resolveSnapshot(snapshot), EMPTY_READ_OPTIONS);
							Buffer result;
							startedGet.increment();
							try {
								result = getTime.recordCallable(() -> db.get(readOptions, key));
							} finally {
								endedGet.increment();
							}
							logger.trace(MARKER_ROCKSDB, "Read {}: {}", () -> toStringSafe(key), () -> toStringSafe(result));
							if (result != null) {
								sink.next(result.send());
							} else {
								sink.complete();
							}
						} catch (RocksDBException ex) {
							sink.error(new IOException("Failed to read " + toStringSafe(key) + ": " + ex.getMessage()));
						} catch (Exception ex) {
							sink.error(ex);
						}
					}
				});
	}

	@Override
	public Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono, boolean fillCache) {
		return rangeMono.publishOn(dbScheduler).handle((rangeSend, sink) -> {
			try (var range = rangeSend.receive()) {
				assert !Schedulers.isInNonBlockingThread() : "Called isRangeEmpty in a nonblocking thread";
				startedContains.increment();
				try {
					Boolean isRangeEmpty = containsTime.recordCallable(() -> {
						if (range.isSingle()) {
							return !containsKey(snapshot, range.getSingleUnsafe());
						} else {
							// Temporary resources to release after finished
							AbstractSlice<?> slice1 = null;
							AbstractSlice<?> slice2 = null;

							try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
								readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
								readOpts.setFillCache(fillCache);
								if (range.hasMin()) {
									if (nettyDirect && isReadOnlyDirect(range.getMinUnsafe())) {
										readOpts.setIterateLowerBound(slice1 = new DirectSlice(
												((ReadableComponent) range.getMinUnsafe()).readableBuffer(),
												range.getMinUnsafe().readableBytes()
										));
									} else {
										readOpts.setIterateLowerBound(slice1 = new Slice(LLUtils.toArray(range.getMinUnsafe())));
									}
								}
								if (range.hasMax()) {
									if (nettyDirect && isReadOnlyDirect(range.getMaxUnsafe())) {
										readOpts.setIterateUpperBound(slice2 = new DirectSlice(
												((ReadableComponent) range.getMaxUnsafe()).readableBuffer(),
												range.getMaxUnsafe().readableBytes()
										));
									} else {
										readOpts.setIterateUpperBound(slice2 = new Slice(LLUtils.toArray(range.getMaxUnsafe())));
									}
								}
								try (var rocksIterator = db.newIterator(readOpts)) {
									if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
										if (nettyDirect && isReadOnlyDirect(range.getMinUnsafe())) {
											var seekBuf = ((ReadableComponent) range.getMinUnsafe()).readableBuffer();
											rocksIterator.seek(seekBuf);
										} else {
											var seekArray = LLUtils.toArray(range.getMinUnsafe());
											rocksIterator.seek(seekArray);
										}
									} else {
										rocksIterator.seekToFirst();
									}
									return !rocksIterator.isValid();
								}
							} finally {
								if (slice1 != null) {
									slice1.close();
								}
								if (slice2 != null) {
									slice2.close();
								}
							}
						}
					});
					assert isRangeEmpty != null;
					sink.next(isRangeEmpty);
				} catch (RocksDBException ex) {
					sink.error(new RocksDBException("Failed to read range " + LLUtils.toStringSafe(range)
							+ ": " + ex.getMessage()));
				} finally {
					endedContains.increment();
				}
			} catch (Throwable ex) {
				sink.error(ex);
			}
		});
	}

	private boolean containsKey(@Nullable LLSnapshot snapshot, Buffer key) throws RocksDBException {
		startedContains.increment();
		try {
			var result = containsTime.recordCallable(() -> {
				var unmodifiableReadOpts = resolveSnapshot(snapshot);
				return db.exists(unmodifiableReadOpts, key);
			});
			assert result != null;
			return result;
		} catch (RocksDBException | RuntimeException e) {
			throw e;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			endedContains.increment();
		}
	}

	@Override
	public Mono<Send<Buffer>> put(Mono<Send<Buffer>> keyMono, Mono<Send<Buffer>> valueMono,
			LLDictionaryResultType resultType) {
		// Zip the entry to write to the database
		var entryMono = Mono.zip(keyMono, valueMono, Map::entry);
		// Obtain the previous value from the database
		var previousDataMono = this.getPreviousData(keyMono, resultType);
		// Write the new entry to the database
		Mono<Send<Buffer>> putMono = entryMono
				.publishOn(dbScheduler)
				.handle((entry, sink) -> {
					try (var key = entry.getKey().receive()) {
						try (var value = entry.getValue().receive()) {
							put(key, value);
							sink.complete();
						} catch (RocksDBException ex) {
							sink.error(ex);
						}
					}
				});
		// Read the previous data, then write the new data, then return the previous data
		return Flux.concat(previousDataMono, putMono).singleOrEmpty();
	}

	private void put(Buffer key, Buffer value) throws RocksDBException {
		assert key.isAccessible();
		assert value.isAccessible();
		if (logger.isTraceEnabled(MARKER_ROCKSDB)) {
			var varargs = new Supplier<?>[]{() -> toStringSafe(key), () -> toStringSafe(value)};
			logger.trace(MARKER_ROCKSDB, "Writing {}: {}", varargs);
		}
		startedPut.increment();
		try {
			putTime.recordCallable(() -> {
				db.put(EMPTY_WRITE_OPTIONS, key, value);
				return null;
			});
		} catch (RocksDBException ex) {
			throw new RocksDBException("Failed to write: " + ex.getMessage());
		} catch (RuntimeException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to write", ex);
		} finally {
			endedPut.increment();
		}
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return Mono.just(updateMode);
	}

	@SuppressWarnings("DuplicatedCode")
	@Override
	public Mono<Send<Buffer>> update(Mono<Send<Buffer>> keyMono,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
			UpdateReturnMode updateReturnMode) {
		return keyMono
				.publishOn(dbScheduler)
				.handle((keySend, sink) -> {
					try (keySend) {
						assert !Schedulers.isInNonBlockingThread() : "Called update in a nonblocking thread";
						if (updateMode == UpdateMode.DISALLOW) {
							sink.error(new UnsupportedOperationException("update() is disallowed"));
							return;
						}
						UpdateAtomicResultMode returnMode = switch (updateReturnMode) {
							case NOTHING -> UpdateAtomicResultMode.NOTHING;
							case GET_NEW_VALUE -> UpdateAtomicResultMode.CURRENT;
							case GET_OLD_VALUE -> UpdateAtomicResultMode.PREVIOUS;
						};
						UpdateAtomicResult result;
						startedUpdates.increment();
						try {
							result = updateTime.recordCallable(() -> db.updateAtomic(EMPTY_READ_OPTIONS,
									EMPTY_WRITE_OPTIONS, keySend, updater, returnMode));
						} finally {
							endedUpdates.increment();
						}
						assert result != null;
						var previous = switch (updateReturnMode) {
							case NOTHING -> null;
							case GET_NEW_VALUE -> ((UpdateAtomicResultCurrent) result).current();
							case GET_OLD_VALUE -> ((UpdateAtomicResultPrevious) result).previous();
						};
						if (previous != null) {
							sink.next(previous);
						} else {
							sink.complete();
						}
					} catch (Exception ex) {
						sink.error(ex);
					}
				});
	}

	@SuppressWarnings("DuplicatedCode")
	@Override
	public Mono<Send<LLDelta>> updateAndGetDelta(Mono<Send<Buffer>> keyMono,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater) {
		return keyMono
				.publishOn(dbScheduler)
				.handle((keySend, sink) -> {
					try (keySend) {
						assert !Schedulers.isInNonBlockingThread() : "Called update in a nonblocking thread";
						if (updateMode == UpdateMode.DISALLOW) {
							sink.error(new UnsupportedOperationException("update() is disallowed"));
							return;
						}
						if (updateMode == UpdateMode.ALLOW && !db.supportsTransactions()) {
							sink.error(new UnsupportedOperationException("update() is disallowed because the database doesn't support"
									+ "safe atomic operations"));
							return;
						}

						UpdateAtomicResult result;
						startedUpdates.increment();
						try {
							result = updateTime.recordCallable(() -> db.updateAtomic(EMPTY_READ_OPTIONS,
									EMPTY_WRITE_OPTIONS, keySend, updater, UpdateAtomicResultMode.DELTA));
						} finally {
							endedUpdates.increment();
						}
						assert result != null;
						sink.next(((UpdateAtomicResultDelta) result).delta());
					} catch (Exception ex) {
						sink.error(ex);
					}
				});
	}

	@Override
	public Mono<Send<Buffer>> remove(Mono<Send<Buffer>> keyMono, LLDictionaryResultType resultType) {
		// Obtain the previous value from the database
		Mono<Send<Buffer>> previousDataMono = this.getPreviousData(keyMono, resultType);
		// Delete the value from the database
		Mono<Send<Buffer>> removeMono = keyMono
				.publishOn(dbScheduler)
				.handle((keySend, sink) -> {
					try (var key = keySend.receive()) {
						logger.trace(MARKER_ROCKSDB, "Deleting {}", () -> toStringSafe(key));
						startedRemove.increment();
						try {
							removeTime.recordCallable(() -> {
								db.delete(EMPTY_WRITE_OPTIONS, key);
								return null;
							});
						} finally {
							endedRemove.increment();
						}
						sink.complete();
					} catch (RocksDBException ex) {
						sink.error(new RocksDBException("Failed to delete: " + ex.getMessage()));
					} catch (Exception ex) {
						sink.error(ex);
					}
				});
		// Read the previous data, then delete the data, then return the previous data
		return Flux.concat(previousDataMono, removeMono).singleOrEmpty();
	}

	private Mono<Send<Buffer>> getPreviousData(Mono<Send<Buffer>> keyMono, LLDictionaryResultType resultType) {
		return switch (resultType) {
			case PREVIOUS_VALUE_EXISTENCE -> keyMono
					.publishOn(dbScheduler)
					.handle((keySend, sink) -> {
						try (var key = keySend.receive()) {
							var contained = containsKey(null, key);
							sink.next(LLUtils.booleanToResponseByteBuffer(alloc, contained));
						} catch (RocksDBException ex) {
							sink.error(ex);
						}
					});
			case PREVIOUS_VALUE -> keyMono
					.publishOn(dbScheduler)
					.handle((keySend, sink) -> {
						try (var key = keySend.receive()) {
							assert !Schedulers.isInNonBlockingThread() : "Called getPreviousData in a nonblocking thread";
							var result = db.get(EMPTY_READ_OPTIONS, key);
							logger.trace(MARKER_ROCKSDB, "Reading {}: {}", () -> toStringSafe(key), () -> toStringSafe(result));
							if (result == null) {
								sink.complete();
							} else {
								sink.next(result.send());
							}
						} catch (Exception ex) {
							sink.error(ex);
						}
					});
			case VOID -> Mono.empty();
		};
	}

	@Override
	public Flux<Optional<Buffer>> getMulti(@Nullable LLSnapshot snapshot, Flux<Send<Buffer>> keys) {
		return keys
				.buffer(MULTI_GET_WINDOW)
				.publishOn(dbScheduler)
				.<ArrayList<Optional<Buffer>>>handle((keysWindow, sink) -> {
					List<Buffer> keyBufsWindow = new ArrayList<>(keysWindow.size());
					for (Send<Buffer> bufferSend : keysWindow) {
						keyBufsWindow.add(bufferSend.receive());
					}
					try {
						assert !Schedulers.isInNonBlockingThread() : "Called getMulti in a nonblocking thread";
						var readOptions = Objects.requireNonNullElse(resolveSnapshot(snapshot), EMPTY_READ_OPTIONS);
						List<byte[]> results = db.multiGetAsList(readOptions, LLUtils.toArray(keyBufsWindow));
						var mappedResults = new ArrayList<Optional<Buffer>>(results.size());
						for (int i = 0; i < results.size(); i++) {
							byte[] val = results.get(i);
							Optional<Buffer> valueOpt;
							if (val != null) {
								// free memory
								results.set(i, null);

								valueOpt = Optional.of(LLUtils.fromByteArray(alloc, val));
							} else {
								valueOpt = Optional.empty();
							}
							mappedResults.add(valueOpt);
						}
						sink.next(mappedResults);
					} catch (RocksDBException ex) {
						sink.error(new RocksDBException("Failed to read keys: " + ex.getMessage()));
					} finally {
						for (Buffer buffer : keyBufsWindow) {
							buffer.close();
						}
					}
				})
				.flatMapIterable(list -> list);
	}

	@Override
	public Mono<Void> putMulti(Flux<Send<LLEntry>> entries) {
		return entries
				.buffer(Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.publishOn(dbScheduler)
				.handle((entriesWindowList, sink) -> {
					var entriesWindow = new ArrayList<LLEntry>(entriesWindowList.size());
					for (Send<LLEntry> entrySend : entriesWindowList) {
						entriesWindow.add(entrySend.receive());
					}
					try {
						assert !Schedulers.isInNonBlockingThread() : "Called putMulti in a nonblocking thread";
						if (USE_WRITE_BATCHES_IN_PUT_MULTI) {
							var batch = new CappedWriteBatch(db,
									alloc,
									CAPPED_WRITE_BATCH_CAP,
									RESERVED_WRITE_BATCH_SIZE,
									MAX_WRITE_BATCH_SIZE,
									BATCH_WRITE_OPTIONS
							);
							for (LLEntry entry : entriesWindow) {
								var k = entry.getKey();
								var v = entry.getValue();
								if (nettyDirect) {
									batch.put(cfh, k, v);
								} else {
									try (var key = k.receive()) {
										try (var value = v.receive()) {
											batch.put(cfh, LLUtils.toArray(key), LLUtils.toArray(value));
										}
									}
								}
							}
							batch.writeToDbAndClose();
							batch.close();
						} else {
							for (LLEntry entry : entriesWindow) {
								db.put(EMPTY_WRITE_OPTIONS, entry.getKeyUnsafe(), entry.getValueUnsafe());
							}
						}
						sink.complete();
					} catch (RocksDBException ex) {
						sink.error(new RocksDBException("Failed to write: " + ex.getMessage()));
					} finally {
						for (LLEntry llEntry : entriesWindow) {
							llEntry.close();
						}
					}
				})
				.then();
	}

	@Override
	public <K> Flux<Boolean> updateMulti(Flux<K> keys, Flux<Send<Buffer>> serializedKeys,
			KVSerializationFunction<K, @Nullable Send<Buffer>, @Nullable Buffer> updateFunction) {
		return Flux.zip(keys, serializedKeys)
				.buffer(Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.flatMapSequential(ew -> this.<List<Boolean>>runOnDb(() -> {
					List<Tuple2<K, Buffer>> entriesWindow = new ArrayList<>(ew.size());
					for (Tuple2<K, Send<Buffer>> tuple : ew) {
						entriesWindow.add(tuple.mapT2(Send::receive));
					}
					try {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called updateMulti in a nonblocking thread");
						}
						List<Buffer> keyBufsWindow = new ArrayList<>(entriesWindow.size());
						for (Tuple2<K, Buffer> objects : entriesWindow) {
							keyBufsWindow.add(objects.getT2());
						}
						ArrayList<Tuple3<K, Send<Buffer>, Optional<Send<Buffer>>>> mappedInputs;
						{
							var readOptions = Objects.requireNonNullElse(resolveSnapshot(null), EMPTY_READ_OPTIONS);
							var inputs = db.multiGetAsList(readOptions, LLUtils.toArray(keyBufsWindow));
							mappedInputs = new ArrayList<>(inputs.size());
							for (int i = 0; i < inputs.size(); i++) {
								var val = inputs.get(i);
								if (val != null) {
									inputs.set(i, null);
									mappedInputs.add(Tuples.of(
											entriesWindow.get(i).getT1(),
											keyBufsWindow.get(i).send(),
											Optional.of(fromByteArray(alloc, val).send())
									));
								} else {
									mappedInputs.add(Tuples.of(
											entriesWindow.get(i).getT1(),
											keyBufsWindow.get(i).send(),
											Optional.empty()
									));
								}
							}
						}
						var updatedValuesToWrite = new ArrayList<Buffer>(mappedInputs.size());
						var valueChangedResult = new ArrayList<Boolean>(mappedInputs.size());
						try {
							for (var mappedInput : mappedInputs) {
								var updatedValue = updateFunction.apply(mappedInput.getT1(), mappedInput.getT2());
								try {
									if (updatedValue != null) {
										try (var t3 = mappedInput.getT3().map(Send::receive).orElse(null)) {
											valueChangedResult.add(!LLUtils.equals(t3, updatedValue));
										}
										updatedValuesToWrite.add(updatedValue);
									} else {
										try (var t3 = mappedInput.getT3().map(Send::receive).orElse(null)) {
											valueChangedResult.add(!LLUtils.equals(t3, null));
										}
										updatedValuesToWrite.add(null);
									}
								} catch (Throwable t) {
									if (updatedValue != null) {
										updatedValue.close();
									}
									throw t;
								}
							}
						} finally {
							for (var mappedInput : mappedInputs) {
								mappedInput.getT3().ifPresent(Send::close);
							}
						}

						if (USE_WRITE_BATCHES_IN_PUT_MULTI) {
							var batch = new CappedWriteBatch(db,
									alloc,
									CAPPED_WRITE_BATCH_CAP,
									RESERVED_WRITE_BATCH_SIZE,
									MAX_WRITE_BATCH_SIZE,
									BATCH_WRITE_OPTIONS
							);
							int i = 0;
							for (Tuple2<K, Buffer> entry : entriesWindow) {
								try (var valueToWrite = updatedValuesToWrite.get(i)) {
									if (valueToWrite == null) {
										batch.delete(cfh, entry.getT2().send());
									} else {
										batch.put(cfh, entry.getT2().send(), valueToWrite.send());
									}
								}
								i++;
							}
							batch.writeToDbAndClose();
							batch.close();
						} else {
							int i = 0;
							for (Tuple2<K, Buffer> entry : entriesWindow) {
								db.put(EMPTY_WRITE_OPTIONS, entry.getT2(), updatedValuesToWrite.get(i));
								i++;
							}
						}
						return valueChangedResult;
					} finally {
						for (Tuple2<K, Buffer> tuple : entriesWindow) {
							tuple.getT2().close();
						}
					}
				}).flatMapIterable(list -> list), /* Max concurrency is 2 to update data while preparing the next segment */ 2);
	}

	@Override
	public Flux<Send<LLEntry>> getRange(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			boolean reverse,
			boolean smallRange) {
		return rangeMono.flatMapMany(rangeSend -> {
			try (var range = rangeSend.receive()) {
				if (range.isSingle()) {
					var rangeSingleMono = rangeMono.map(r -> r.receive().getSingle());
					return getRangeSingle(snapshot, rangeSingleMono);
				} else {
					return getRangeMulti(snapshot, rangeMono, reverse, smallRange);
				}
			}
		});
	}

	@Override
	public Flux<List<Send<LLEntry>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			int prefixLength,
			boolean smallRange) {
		return rangeMono.flatMapMany(rangeSend -> {
			try (var range = rangeSend.receive()) {
				if (range.isSingle()) {
					var rangeSingleMono = rangeMono.map(r -> r.receive().getSingle());
					return getRangeSingle(snapshot, rangeSingleMono).map(List::of);
				} else {
					return getRangeMultiGrouped(snapshot, rangeMono, prefixLength, smallRange);
				}
			}
		});
	}

	private Flux<Send<LLEntry>> getRangeSingle(LLSnapshot snapshot, Mono<Send<Buffer>> keyMono) {
		return Mono
				.zip(keyMono, this.get(snapshot, keyMono))
				.map(result -> LLEntry.of(result.getT1(), result.getT2()).send())
				.flux();
	}

	private Flux<Send<LLEntry>> getRangeMulti(LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			boolean reverse,
			boolean smallRange) {
		Mono<LLLocalEntryReactiveRocksIterator> iteratorMono = rangeMono.map(rangeSend -> {
			ReadOptions resolvedSnapshot = resolveSnapshot(snapshot);
			return new LLLocalEntryReactiveRocksIterator(db, rangeSend, nettyDirect, resolvedSnapshot, reverse, smallRange);
		});
		return Flux.usingWhen(iteratorMono,
				iterator -> iterator.flux().subscribeOn(dbScheduler, false),
				iterator -> Mono.fromRunnable(iterator::close)
		);
	}

	private Flux<List<Send<LLEntry>>> getRangeMultiGrouped(LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono,
			int prefixLength, boolean smallRange) {
		Mono<LLLocalGroupedEntryReactiveRocksIterator> iteratorMono = rangeMono.map(rangeSend -> {
			ReadOptions resolvedSnapshot = resolveSnapshot(snapshot);
			return new LLLocalGroupedEntryReactiveRocksIterator(db,
					prefixLength,
					rangeSend,
					nettyDirect,
					resolvedSnapshot,
					smallRange
			);
		});
		return Flux.usingWhen(
				iteratorMono,
				iterator -> iterator.flux().subscribeOn(dbScheduler, false),
				iterator -> Mono.fromRunnable(iterator::close)
		);
	}

	@Override
	public Flux<Send<Buffer>> getRangeKeys(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			boolean reverse,
			boolean smallRange) {
		return rangeMono.flatMapMany(rangeSend -> {
			try (var range = rangeSend.receive()) {
				if (range.isSingle()) {
					return this.getRangeKeysSingle(snapshot, rangeMono.map(r -> r.receive().getSingle()));
				} else {
					return this.getRangeKeysMulti(snapshot, rangeMono, reverse, smallRange);
				}
			}
		});
	}

	@Override
	public Flux<List<Send<Buffer>>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			int prefixLength,
			boolean smallRange) {
		Mono<LLLocalGroupedKeyReactiveRocksIterator> iteratorMono = rangeMono.map(rangeSend -> {
			ReadOptions resolvedSnapshot = resolveSnapshot(snapshot);
			return new LLLocalGroupedKeyReactiveRocksIterator(db,
					prefixLength,
					rangeSend,
					nettyDirect,
					resolvedSnapshot,
					smallRange
			);
		});
		return Flux.usingWhen(iteratorMono,
				iterator -> iterator.flux().subscribeOn(dbScheduler, false),
				iterator -> Mono.fromRunnable(iterator::close)
		);
	}

	@Override
	public Flux<BadBlock> badBlocks(Mono<Send<LLRange>> rangeMono) {
		return Flux.usingWhen(rangeMono,
				rangeSend -> Flux
						.<BadBlock>create(sink -> {
							var range = rangeSend.receive();
							sink.onDispose(range::close);
							try (var ro = new ReadOptions(getReadOptions(null))) {
								ro.setFillCache(false);
								if (!range.isSingle()) {
									if (LLUtils.MANUAL_READAHEAD) {
										ro.setReadaheadSize(32 * 1024);
									}
								}
								ro.setVerifyChecksums(true);
								try (var rocksIteratorTuple = db.getRocksIterator(nettyDirect, ro, range, false)) {
									var rocksIterator = rocksIteratorTuple.iterator();
									rocksIterator.seekToFirst();
									while (rocksIterator.isValid() && !sink.isCancelled()) {
										try {
											rocksIterator.key(DUMMY_WRITE_ONLY_BYTE_BUFFER);
											rocksIterator.value(DUMMY_WRITE_ONLY_BYTE_BUFFER);
											rocksIterator.next();
										} catch (RocksDBException ex) {
											sink.next(new BadBlock(databaseName, ColumnUtils.special(columnName), null, ex));
										}
									}
								}
								sink.complete();
							} catch (Throwable ex) {
								sink.error(ex);
							}
						})
						.subscribeOn(dbScheduler),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	@Override
	public Flux<Send<Buffer>> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono,
			int prefixLength, boolean smallRange) {
		Mono<LLLocalKeyPrefixReactiveRocksIterator> iteratorMono = rangeMono.map(range -> {
			ReadOptions resolvedSnapshot = resolveSnapshot(snapshot);
			return new LLLocalKeyPrefixReactiveRocksIterator(db, prefixLength, range, nettyDirect, resolvedSnapshot, true,
					smallRange
			);
		});
		return Flux.usingWhen(iteratorMono,
				iterator -> iterator.flux().subscribeOn(dbScheduler),
				iterator -> Mono.fromRunnable(iterator::close)
		);
	}

	private Flux<Send<Buffer>> getRangeKeysSingle(LLSnapshot snapshot, Mono<Send<Buffer>> keyMono) {
		return keyMono
				.publishOn(dbScheduler)
				.<Send<Buffer>>handle((keySend, sink) -> {
					try (var key = keySend.receive()) {
						if (containsKey(snapshot, key)) {
							sink.next(key.send());
						} else {
							sink.complete();
						}
					} catch (Throwable ex) {
						sink.error(ex);
					}
				})
				.flux();
	}

	private Flux<Send<Buffer>> getRangeKeysMulti(LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			boolean reverse,
			boolean smallRange) {
		Mono<LLLocalKeyReactiveRocksIterator> iteratorMono = rangeMono.map(range -> {
			ReadOptions resolvedSnapshot = resolveSnapshot(snapshot);
			return new LLLocalKeyReactiveRocksIterator(db, range, nettyDirect, resolvedSnapshot, reverse, smallRange);
		});
		return Flux.usingWhen(iteratorMono,
				iterator -> iterator.flux().subscribeOn(dbScheduler, false),
				iterator -> Mono.fromRunnable(iterator::close)
		);
	}

	@Override
	public Mono<Void> setRange(Mono<Send<LLRange>> rangeMono, Flux<Send<LLEntry>> entries, boolean smallRange) {
		if (USE_WINDOW_IN_SET_RANGE) {
			return rangeMono
					.publishOn(dbScheduler)
					.<Void>handle((rangeSend, sink) -> {
						try (var range = rangeSend.receive()) {
							assert !Schedulers.isInNonBlockingThread() : "Called setRange in a nonblocking thread";
							if (!USE_WRITE_BATCH_IN_SET_RANGE_DELETE || !USE_WRITE_BATCHES_IN_SET_RANGE) {
								assert EMPTY_READ_OPTIONS.isOwningHandle();
								try (var opts = new ReadOptions(EMPTY_READ_OPTIONS)) {
									ReleasableSlice minBound;
									if (range.hasMin()) {
										minBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, opts, IterateBound.LOWER, range.getMinUnsafe());
									} else {
										minBound = AbstractRocksDBColumn.emptyReleasableSlice();
									}
									try {
										ReleasableSlice maxBound;
										if (range.hasMax()) {
											maxBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, opts, IterateBound.UPPER, range.getMaxUnsafe());
										} else {
											maxBound = AbstractRocksDBColumn.emptyReleasableSlice();
										}
										assert cfh.isOwningHandle();
										assert opts.isOwningHandle();
										SafeCloseable seekTo;
										try (var it = db.newIterator(opts)) {
											if (!PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
												seekTo = it.seekTo(range.getMinUnsafe());
											} else {
												seekTo = null;
												it.seekToFirst();
											}
											try {
												while (it.isValid()) {
													db.delete(EMPTY_WRITE_OPTIONS, it.key());
													it.next();
												}
											} finally {
												if (seekTo != null) {
													seekTo.close();
												}
											}
										} finally {
											maxBound.close();
										}
									} finally {
										minBound.close();
									}
								}
							} else if (USE_CAPPED_WRITE_BATCH_IN_SET_RANGE) {
								try (var batch = new CappedWriteBatch(db,
										alloc,
										CAPPED_WRITE_BATCH_CAP,
										RESERVED_WRITE_BATCH_SIZE,
										MAX_WRITE_BATCH_SIZE,
										BATCH_WRITE_OPTIONS
								)) {
									if (range.isSingle()) {
										batch.delete(cfh, range.getSingle());
									} else {
										deleteSmallRangeWriteBatch(batch, range.copy().send());
									}
									batch.writeToDbAndClose();
								}
							} else {
								try (var batch = new WriteBatch(RESERVED_WRITE_BATCH_SIZE)) {
									if (range.isSingle()) {
										batch.delete(cfh, LLUtils.toArray(range.getSingleUnsafe()));
									} else {
										deleteSmallRangeWriteBatch(batch, range.copy().send());
									}
									db.write(EMPTY_WRITE_OPTIONS, batch);
									batch.clear();
								}
							}
							sink.complete();
						} catch (RocksDBException ex) {
							sink.error(new RocksDBException("Failed to set a range: " + ex.getMessage()));
						}
					})
					.thenMany(entries.window(MULTI_GET_WINDOW))
					.flatMap(keysWindowFlux -> keysWindowFlux
							.collectList()
							.flatMap(entriesListSend -> this
									.<Void>runOnDb(() -> {
										List<LLEntry> entriesList = new ArrayList<>(entriesListSend.size());
										for (Send<LLEntry> entrySend : entriesListSend) {
											entriesList.add(entrySend.receive());
										}
										try {
											if (!USE_WRITE_BATCHES_IN_SET_RANGE) {
												for (LLEntry entry : entriesList) {
													assert entry.isAccessible();
													db.put(EMPTY_WRITE_OPTIONS, entry.getKeyUnsafe(), entry.getValueUnsafe());
												}
											} else if (USE_CAPPED_WRITE_BATCH_IN_SET_RANGE) {
												try (var batch = new CappedWriteBatch(db,
														alloc,
														CAPPED_WRITE_BATCH_CAP,
														RESERVED_WRITE_BATCH_SIZE,
														MAX_WRITE_BATCH_SIZE,
														BATCH_WRITE_OPTIONS
												)) {
													for (LLEntry entry : entriesList) {
														assert entry.isAccessible();
														if (nettyDirect) {
															batch.put(cfh, entry.getKey(), entry.getValue());
														} else {
															batch.put(cfh,
																	LLUtils.toArray(entry.getKeyUnsafe()),
																	LLUtils.toArray(entry.getValueUnsafe())
															);
														}
													}
													batch.writeToDbAndClose();
												}
											} else {
												try (var batch = new WriteBatch(RESERVED_WRITE_BATCH_SIZE)) {
													for (LLEntry entry : entriesList) {
														assert entry.isAccessible();
														batch.put(cfh, LLUtils.toArray(entry.getKeyUnsafe()),
																LLUtils.toArray(entry.getValueUnsafe()));
													}
													db.write(EMPTY_WRITE_OPTIONS, batch);
													batch.clear();
												}
											}
											return null;
										} finally {
											for (LLEntry entry : entriesList) {
												assert entry.isAccessible();
												entry.close();
											}
										}
									})
							)
					)
					.then()
					.onErrorMap(cause -> new IOException("Failed to write range", cause));
		} else {
			if (USE_WRITE_BATCHES_IN_SET_RANGE) {
				return Mono.error(() -> new UnsupportedOperationException(
						"Can't use write batches in setRange without window. Please fix the parameters"));
			}
			var deleteMono = this
					.getRange(null, rangeMono, false, smallRange)
					.publishOn(dbScheduler)
					.handle((oldValueSend, sink) -> {
						try (var oldValue = oldValueSend.receive()) {
							db.delete(EMPTY_WRITE_OPTIONS, oldValue.getKeyUnsafe());
							sink.complete();
						} catch (RocksDBException ex) {
							sink.error(new RocksDBException("Failed to write range: " + ex.getMessage()));
						}
					})
					.then(Mono.<Void>empty());

			var putMono = entries
					.publishOn(dbScheduler)
					.handle((entrySend, sink) -> {
						try (var entry = entrySend.receive()) {
							if (entry.getKeyUnsafe() != null && entry.getValueUnsafe() != null) {
								this.put(entry.getKeyUnsafe(), entry.getValueUnsafe());
							}
							sink.complete();
						} catch (RocksDBException ex) {
							sink.error(new RocksDBException("Failed to write range: " + ex.getMessage()));
						}
					})
					.then(Mono.<Void>empty());

			return deleteMono.then(putMono);
		}
	}

	//todo: this is broken, check why. (is this still true?)
	private void deleteSmallRangeWriteBatch(CappedWriteBatch writeBatch, Send<LLRange> rangeToReceive)
			throws RocksDBException {
		var range = rangeToReceive.receive();
		try (var readOpts = new ReadOptions(getReadOptions(null))) {
			readOpts.setFillCache(false);
			ReleasableSlice minBound;
			if (range.hasMin()) {
				minBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
			} else {
				minBound = AbstractRocksDBColumn.emptyReleasableSlice();
			}
			try {
				ReleasableSlice maxBound;
				if (range.hasMax()) {
					maxBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
				} else {
					maxBound = AbstractRocksDBColumn.emptyReleasableSlice();
				}
				try (var rocksIterator = db.newIterator(readOpts)) {
					SafeCloseable seekTo;
					if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
						seekTo = rocksIterator.seekTo(range.getMinUnsafe());
					} else {
						seekTo = null;
						rocksIterator.seekToFirst();
					}
					try {
						while (rocksIterator.isValid()) {
							writeBatch.delete(cfh, LLUtils.readDirectNioBuffer(alloc, rocksIterator::key).send());
							rocksIterator.next();
						}
					} finally {
						if (seekTo != null) {
							seekTo.close();
						}
					}
				} finally {
					maxBound.close();
				}
			} finally {
				minBound.close();
			}
		} catch (Throwable e) {
			range.close();
			throw e;
		}
	}

	private void deleteSmallRangeWriteBatch(WriteBatch writeBatch, Send<LLRange> rangeToReceive)
			throws RocksDBException {
		try (var range = rangeToReceive.receive()) {
			try (var readOpts = new ReadOptions(getReadOptions(null))) {
				readOpts.setFillCache(false);
				ReleasableSlice minBound;
				if (range.hasMin()) {
					minBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
				} else {
					minBound = AbstractRocksDBColumn.emptyReleasableSlice();
				}
				try {
					ReleasableSlice maxBound;
					if (range.hasMax()) {
						maxBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
					} else {
						maxBound = AbstractRocksDBColumn.emptyReleasableSlice();
					}
					try (var rocksIterator = db.newIterator(readOpts)) {
						SafeCloseable seekTo;
						if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
							seekTo = rocksIterator.seekTo(range.getMinUnsafe());
						} else {
							seekTo = null;
							rocksIterator.seekToFirst();
						}
						try {
							while (rocksIterator.isValid()) {
								writeBatch.delete(cfh, rocksIterator.key());
								rocksIterator.next();
							}
						} finally {
							if (seekTo != null) {
								seekTo.close();
							}
						}
					} finally {
						maxBound.close();
					}
				} finally {
					minBound.close();
				}
			}
		}
	}

	/**
	 * This method should not modify or move the writerIndex/readerIndex of the key
	 */
	public record ReleasableSliceImplWithoutRelease(AbstractSlice<?> slice) implements ReleasableSlice {}

	/**
	 * This class should not modify or move the writerIndex/readerIndex of the key
	 */
	public record ReleasableSliceImplWithRelease(AbstractSlice<?> slice) implements ReleasableSlice {

		@Override
		public void close() {
			slice.close();
		}
	}

	public Mono<Void> clear() {
		return Mono
				.<Void>fromCallable(() -> {
					assert !Schedulers.isInNonBlockingThread() : "Called clear in a nonblocking thread";
					boolean shouldCompactLater = false;
					try (var readOpts = new ReadOptions(getReadOptions(null))) {
						readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);

						// readOpts.setIgnoreRangeDeletions(true);
						readOpts.setFillCache(false);
						if (LLUtils.MANUAL_READAHEAD) {
							readOpts.setReadaheadSize(32 * 1024); // 32KiB
						}
						try (CappedWriteBatch writeBatch = new CappedWriteBatch(db,
								alloc,
								CAPPED_WRITE_BATCH_CAP,
								RESERVED_WRITE_BATCH_SIZE,
								MAX_WRITE_BATCH_SIZE,
								BATCH_WRITE_OPTIONS
						)) {

							byte[] firstDeletedKey = null;
							byte[] lastDeletedKey = null;
							try (var rocksIterator = db.newIterator(readOpts)) {
								// If the database supports transactions, delete each key one by one
								if (db.supportsTransactions()) {
									rocksIterator.seekToFirst();
									while (rocksIterator.isValid()) {
										writeBatch.delete(cfh, rocksIterator.key());
										rocksIterator.next();
									}
								} else {
									rocksIterator.seekToLast();

									if (rocksIterator.isValid()) {
										firstDeletedKey = FIRST_KEY;
										lastDeletedKey = rocksIterator.key();
										writeBatch.deleteRange(cfh, FIRST_KEY, rocksIterator.key());
										writeBatch.delete(cfh, rocksIterator.key());
										shouldCompactLater = true;
									}
								}
							}

							writeBatch.writeToDbAndClose();

							if (shouldCompactLater) {
								// Compact range
								db.suggestCompactRange();
								if (lastDeletedKey != null) {
									db.compactRange(firstDeletedKey, lastDeletedKey, new CompactRangeOptions()
											.setAllowWriteStall(false)
											.setExclusiveManualCompaction(false)
											.setChangeLevel(false)
									);
								}
							}

							db.flush(new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true));
							db.flushWal(true);
						}
						return null;
					}
				})
				.onErrorMap(cause -> new IOException("Failed to clear", cause))
				.subscribeOn(dbScheduler);

	}

	@Override
	public Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono, boolean fast) {
		return rangeMono.publishOn(dbScheduler).handle((rangeSend, sink) -> {
			try (var range = rangeSend.receive()) {
				assert !Schedulers.isInNonBlockingThread() : "Called sizeRange in a nonblocking thread";
				if (range.isAll()) {
					sink.next(fast ? fastSizeAll(snapshot) : exactSizeAll(snapshot));
				} else {
					try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
						readOpts.setFillCache(false);
						readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
						ReleasableSlice minBound;
						if (range.hasMin()) {
							minBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
						} else {
							minBound = AbstractRocksDBColumn.emptyReleasableSlice();
						}
						try {
							ReleasableSlice maxBound;
							if (range.hasMax()) {
								maxBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
							} else {
								maxBound = AbstractRocksDBColumn.emptyReleasableSlice();
							}
							try {
								if (fast) {
									readOpts.setIgnoreRangeDeletions(true);

								}
								try (var rocksIterator = db.newIterator(readOpts)) {
									SafeCloseable seekTo;
									if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
										seekTo = rocksIterator.seekTo(range.getMinUnsafe());
									} else {
										seekTo = null;
										rocksIterator.seekToFirst();
									}
									try {
										long i = 0;
										while (rocksIterator.isValid()) {
											rocksIterator.next();
											i++;
										}
										sink.next(i);
									} finally {
										if (seekTo != null) {
											seekTo.close();
										}
									}
								}
							} finally {
								maxBound.close();
							}
						} finally {
							minBound.close();
						}
					}
				}
			} catch (RocksDBException ex) {
				sink.error(new RocksDBException("Failed to get size of range: " + ex.getMessage()));
			}
		});
	}

	@Override
	public Mono<Send<LLEntry>> getOne(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return rangeMono.publishOn(dbScheduler).handle((rangeSend, sink) -> {
			try (var range = rangeSend.receive()) {
				assert !Schedulers.isInNonBlockingThread() : "Called getOne in a nonblocking thread";
				try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
					ReleasableSlice minBound;
					if (range.hasMin()) {
						minBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
					} else {
						minBound = AbstractRocksDBColumn.emptyReleasableSlice();
					}
					try {
						ReleasableSlice maxBound;
						if (range.hasMax()) {
							maxBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
						} else {
							maxBound = AbstractRocksDBColumn.emptyReleasableSlice();
						}
						try (var rocksIterator = db.newIterator(readOpts)) {
							SafeCloseable seekTo;
							if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
								seekTo = rocksIterator.seekTo(range.getMinUnsafe());
							} else {
								seekTo = null;
								rocksIterator.seekToFirst();
							}
							try {
								if (rocksIterator.isValid()) {
									try (var key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key)) {
										try (var value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value)) {
											sink.next(LLEntry.of(key.send(), value.send()).send());
										}
									}
								} else {
									sink.complete();
								}
							} finally {
								if (seekTo != null) {
									seekTo.close();
								}
							}
						} finally {
							maxBound.close();
						}
					} finally {
						minBound.close();
					}
				}
			} catch (RocksDBException ex) {
				sink.error(new RocksDBException("Failed to get one entry: " + ex.getMessage()));
			}
		});
	}

	@Override
	public Mono<Send<Buffer>> getOneKey(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return rangeMono.publishOn(dbScheduler).handle((rangeSend, sink) -> {
			try (var range = rangeSend.receive()) {
				assert !Schedulers.isInNonBlockingThread() : "Called getOneKey in a nonblocking thread";
				try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
					ReleasableSlice minBound;
					if (range.hasMin()) {
						minBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
					} else {
						minBound = AbstractRocksDBColumn.emptyReleasableSlice();
					}
					try {
						ReleasableSlice maxBound;
						if (range.hasMax()) {
							maxBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
						} else {
							maxBound = AbstractRocksDBColumn.emptyReleasableSlice();
						}
						try (var rocksIterator = db.newIterator(readOpts)) {
							SafeCloseable seekTo;
							if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
								seekTo = rocksIterator.seekTo(range.getMinUnsafe());
							} else {
								seekTo = null;
								rocksIterator.seekToFirst();
							}
							try {
								if (rocksIterator.isValid()) {
									sink.next(LLUtils.readDirectNioBuffer(alloc, rocksIterator::key).send());
								} else {
									sink.complete();
								}
							} finally {
								if (seekTo != null) {
									seekTo.close();
								}
							}
						} finally {
							maxBound.close();
						}
					} finally {
						minBound.close();
					}
				}
			} catch (RocksDBException ex) {
				sink.error(new RocksDBException("Failed to get one key: " + ex.getMessage()));
			}
		});
	}

	private long fastSizeAll(@Nullable LLSnapshot snapshot) throws RocksDBException {
		try (var rocksdbSnapshot = new ReadOptions(resolveSnapshot(snapshot))) {
			if (USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS || rocksdbSnapshot.snapshot() == null) {
				try {
					return db.getLongProperty("rocksdb.estimate-num-keys");
				} catch (RocksDBException e) {
					logger.error(MARKER_ROCKSDB, "Failed to get RocksDB estimated keys count property", e);
					return 0;
				}
			} else if (PARALLEL_EXACT_SIZE) {
				return exactSizeAll(snapshot);
			} else {
				rocksdbSnapshot.setFillCache(false);
				rocksdbSnapshot.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
				rocksdbSnapshot.setIgnoreRangeDeletions(true);
				long count = 0;
				try (var rocksIterator = db.newIterator(rocksdbSnapshot)) {
					rocksIterator.seekToFirst();
					// If it's a fast size of a snapshot, count only up to 100'000 elements
					while (rocksIterator.isValid() && count < 100_000) {
						count++;
						rocksIterator.next();
					}
					return count;
				}
			}
		}
	}

	private long exactSizeAll(@Nullable LLSnapshot snapshot) {
		if (Schedulers.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called exactSizeAll in a nonblocking thread");
		}
		try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
			readOpts.setFillCache(false);
			if (LLUtils.MANUAL_READAHEAD) {
				readOpts.setReadaheadSize(128 * 1024); // 128KiB
			}
			readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);

			if (PARALLEL_EXACT_SIZE) {
				var commonPool = ForkJoinPool.commonPool();
				var futures = IntStream
						.range(-1, LLUtils.LEXICONOGRAPHIC_ITERATION_SEEKS.length)
						.mapToObj(idx -> Pair.of(idx == -1 ? new byte[0] : LLUtils.LEXICONOGRAPHIC_ITERATION_SEEKS[idx],
								idx + 1 >= LLUtils.LEXICONOGRAPHIC_ITERATION_SEEKS.length ? null
										: LLUtils.LEXICONOGRAPHIC_ITERATION_SEEKS[idx + 1]
						))
						.map(range -> (Callable<Long>) () -> {
							long partialCount = 0;
							try (var rangeReadOpts = new ReadOptions(readOpts)) {
								Slice sliceBegin;
								if (range.getKey() != null) {
									sliceBegin = new Slice(range.getKey());
								} else {
									sliceBegin = null;
								}
								Slice sliceEnd;
								if (range.getValue() != null) {
									sliceEnd = new Slice(range.getValue());
								} else {
									sliceEnd = null;
								}
								try {
									if (sliceBegin != null) {
										rangeReadOpts.setIterateLowerBound(sliceBegin);
									}
									if (sliceBegin != null) {
										rangeReadOpts.setIterateUpperBound(sliceEnd);
									}
									try (var rocksIterator = db.newIterator(rangeReadOpts)) {
										rocksIterator.seekToFirst();
										while (rocksIterator.isValid()) {
											partialCount++;
											rocksIterator.next();
										}
										return partialCount;
									}
								} finally {
									if (sliceBegin != null) {
										sliceBegin.close();
									}
									if (sliceEnd != null) {
										sliceEnd.close();
									}
								}
							}
						})
						.map(commonPool::submit)
						.toList();
				long count = 0;
				for (ForkJoinTask<Long> future : futures) {
					count += future.join();
				}
				return count;
			} else {
				long count = 0;
				try (var rocksIterator = db.newIterator(readOpts)) {
					rocksIterator.seekToFirst();
					while (rocksIterator.isValid()) {
						count++;
						rocksIterator.next();
					}
					return count;
				} catch (RocksDBException ex) {
					throw new IllegalStateException("Failed to read exact size", ex);
				}
			}
		}
	}

	@Override
	public Mono<Send<LLEntry>> removeOne(Mono<Send<LLRange>> rangeMono) {
		return rangeMono.publishOn(dbScheduler).handle((rangeSend, sink) -> {
			try (var range = rangeSend.receive()) {
				assert !Schedulers.isInNonBlockingThread() : "Called removeOne in a nonblocking thread";
				try (var readOpts = new ReadOptions(getReadOptions(null))) {
					ReleasableSlice minBound;
					if (range.hasMin()) {
						minBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
					} else {
						minBound = AbstractRocksDBColumn.emptyReleasableSlice();
					}
					try {
						ReleasableSlice maxBound;
						if (range.hasMax()) {
							maxBound = AbstractRocksDBColumn.setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
						} else {
							maxBound = AbstractRocksDBColumn.emptyReleasableSlice();
						}
						try (var rocksIterator = db.newIterator(readOpts)) {
							SafeCloseable seekTo;
							if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
								seekTo = rocksIterator.seekTo(range.getMinUnsafe());
							} else {
								seekTo = null;
								rocksIterator.seekToFirst();
							}
							try {
								if (!rocksIterator.isValid()) {
									sink.complete();
									return;
								}
								Buffer key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
								Buffer value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value);
								db.delete(EMPTY_WRITE_OPTIONS, key);
								sink.next(LLEntry.of(key, value).send());
							} finally {
								if (seekTo != null) {
									seekTo.close();
								}
							}
						} finally {
							maxBound.close();
						}
					} finally {
						minBound.close();
					}
				}
			} catch (RocksDBException ex) {
				sink.error(ex);
			}
		});
	}

}
