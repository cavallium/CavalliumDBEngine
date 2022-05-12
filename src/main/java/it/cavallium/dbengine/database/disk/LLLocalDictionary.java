package it.cavallium.dbengine.database.disk;

import static io.netty5.buffer.api.StandardAllocationTypes.OFF_HEAP;
import static it.cavallium.dbengine.database.LLUtils.ALLOW_STATIC_OPTIONS;
import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.fromByteArray;
import static it.cavallium.dbengine.database.LLUtils.isBoundedRange;
import static it.cavallium.dbengine.database.LLUtils.isReadOnlyDirect;
import static it.cavallium.dbengine.database.LLUtils.toStringSafe;
import static it.cavallium.dbengine.database.disk.UpdateAtomicResultMode.DELTA;
import static java.util.Objects.requireNonNull;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.ReadableComponent;
import io.netty5.buffer.api.Resource;
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
import it.cavallium.dbengine.database.disk.rocksdb.RocksObj;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.rocksdb.AbstractNativeReference;
import org.rocksdb.AbstractSlice;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.DirectSlice;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
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
	private static final RocksObj<ReadOptions> EMPTY_READ_OPTIONS = LLUtils.ALLOW_STATIC_OPTIONS ? new RocksObj<>(new ReadOptions()) : null;
	static final boolean PREFER_AUTO_SEEK_BOUND = false;
	/**
	 * It used to be false,
	 * now it's true to avoid crashes during iterations on completely corrupted files
	 */
	static final boolean VERIFY_CHECKSUMS_WHEN_NOT_NEEDED = !LLUtils.FORCE_DISABLE_CHECKSUM_VERIFICATION;
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
	private final RocksObj<ColumnFamilyHandle> cfh;
	private final String databaseName;
	private final String columnName;
	private final Scheduler dbWScheduler;
	private final Scheduler dbRScheduler;
	private final Function<LLSnapshot, RocksObj<Snapshot>> snapshotResolver;
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

	public LLLocalDictionary(BufferAllocator allocator,
			@NotNull RocksDBColumn db,
			String databaseName,
			String columnName,
			Scheduler dbWScheduler,
			Scheduler dbRScheduler,
			Function<LLSnapshot, RocksObj<Snapshot>> snapshotResolver,
			UpdateMode updateMode,
			DatabaseOptions databaseOptions) {
		requireNonNull(db);
		this.db = db;
		this.cfh = db.getColumnFamilyHandle();
		this.databaseName = databaseName;
		this.columnName = columnName;
		this.dbWScheduler = dbWScheduler;
		this.dbRScheduler = dbRScheduler;
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

	@NotNull
	private RocksObj<ReadOptions> generateReadOptionsOrStatic(LLSnapshot snapshot) {
		var resolved = generateReadOptions(snapshot != null ? snapshotResolver.apply(snapshot) : null, true);
		if (resolved != null) {
			return resolved;
		} else {
			return new RocksObj<>(new ReadOptions());
		}
	}

	@Nullable
	private RocksObj<ReadOptions> generateReadOptionsOrNull(LLSnapshot snapshot) {
		return generateReadOptions(snapshot != null ? snapshotResolver.apply(snapshot) : null, false);
	}

	@NotNull
	private RocksObj<ReadOptions> generateReadOptionsOrNew(LLSnapshot snapshot) {
		var result = generateReadOptions(snapshot != null ? snapshotResolver.apply(snapshot) : null, false);
		if (result != null) {
			return result;
		} else {
			return new RocksObj<>(new ReadOptions());
		}
	}

	private RocksObj<ReadOptions> generateReadOptions(RocksObj<Snapshot> snapshot, boolean orStaticOpts) {
		if (snapshot != null) {
			return new RocksObj<>(new ReadOptions().setSnapshot(snapshot.v()));
		} else if (ALLOW_STATIC_OPTIONS && orStaticOpts) {
			return EMPTY_READ_OPTIONS;
		} else {
			return null;
		}
	}

	@Override
	public BufferAllocator getAllocator() {
		return alloc;
	}

	private <T> @NotNull Mono<T> runOnDb(boolean write, Callable<@Nullable T> callable) {
		return Mono.fromCallable(callable).subscribeOn(write ? dbWScheduler : dbRScheduler);
	}

	@Override
	public Mono<Send<Buffer>> get(@Nullable LLSnapshot snapshot, Mono<Send<Buffer>> keyMono) {
		return keyMono
				.publishOn(dbRScheduler)
				.<Send<Buffer>>handle((keySend, sink) -> {
					try (var key = keySend.receive()) {
						logger.trace(MARKER_ROCKSDB, "Reading {}", () -> toStringSafe(key));
						try {
							var readOptions = generateReadOptionsOrStatic(snapshot);
							Buffer result;
							startedGet.increment();
							try {
								result = getTime.recordCallable(() -> db.get(readOptions, key));
							} finally {
								endedGet.increment();
								if (readOptions != EMPTY_READ_OPTIONS) {
									readOptions.close();
								}
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
		return rangeMono.publishOn(dbRScheduler).handle((rangeSend, sink) -> {
			try (var range = rangeSend.receive()) {
				assert !Schedulers.isInNonBlockingThread() : "Called isRangeEmpty in a nonblocking thread";
				startedContains.increment();
				try {
					Boolean isRangeEmpty = containsTime.recordCallable(() -> {
						if (range.isSingle()) {
							return !containsKey(snapshot, range.getSingleUnsafe());
						} else {
							// Temporary resources to release after finished

							try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNull(snapshot),
									true,
									isBoundedRange(range),
									true
							)) {
								readOpts.v().setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
								readOpts.v().setFillCache(fillCache);
								try (var rocksIterator = db.newIterator(readOpts, range.getMinUnsafe(), range.getMaxUnsafe())) {
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
				var readOptions = generateReadOptionsOrStatic(snapshot);
				try {
					return db.exists(readOptions, key);
				} finally {
					if (readOptions != EMPTY_READ_OPTIONS) {
						readOptions.close();
					}
				}
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
				.publishOn(dbWScheduler)
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
		try (var writeOptions = new RocksObj<>(new WriteOptions())) {
			putTime.recordCallable(() -> {
				db.put(writeOptions, key, value);
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
			BinarySerializationFunction updater,
			UpdateReturnMode updateReturnMode) {
		return keyMono
				.publishOn(dbWScheduler)
				.handle((keySend, sink) -> {
					try (var key = keySend.receive()) {
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
						var readOptions = generateReadOptionsOrStatic(null);
						startedUpdates.increment();
						try (var writeOptions = new RocksObj<>(new WriteOptions())) {
							result = updateTime.recordCallable(() ->
									db.updateAtomic(readOptions, writeOptions, key, updater, returnMode));
						} finally {
							endedUpdates.increment();
							if (readOptions != EMPTY_READ_OPTIONS) {
								readOptions.close();
							}
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
			BinarySerializationFunction updater) {
		return keyMono
				.publishOn(dbWScheduler)
				.handle((keySend, sink) -> {
					try (var key = keySend.receive()) {
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
						var readOptions = generateReadOptionsOrStatic(null);
						startedUpdates.increment();
						try (var writeOptions = new RocksObj<>(new WriteOptions())) {
							result = updateTime.recordCallable(() ->
									db.updateAtomic(readOptions, writeOptions, key, updater, DELTA));
						} finally {
							endedUpdates.increment();
							if (readOptions != EMPTY_READ_OPTIONS) {
								readOptions.close();
							}
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
				.publishOn(dbWScheduler)
				.handle((keySend, sink) -> {
					try (var key = keySend.receive()) {
						logger.trace(MARKER_ROCKSDB, "Deleting {}", () -> toStringSafe(key));
						startedRemove.increment();
						try (var writeOptions = new RocksObj<>(new WriteOptions())) {
							removeTime.recordCallable(() -> {
								db.delete(writeOptions, key);
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
					.publishOn(dbRScheduler)
					.handle((keySend, sink) -> {
						try (var key = keySend.receive()) {
							var contained = containsKey(null, key);
							sink.next(LLUtils.booleanToResponseByteBuffer(alloc, contained));
						} catch (RocksDBException ex) {
							sink.error(ex);
						}
					});
			case PREVIOUS_VALUE -> keyMono
					.publishOn(dbRScheduler)
					.handle((keySend, sink) -> {
						try (var key = keySend.receive()) {
							assert !Schedulers.isInNonBlockingThread() : "Called getPreviousData in a nonblocking thread";
							Buffer result;
							var readOptions = generateReadOptionsOrStatic(null);
							try {
								result = db.get(readOptions, key);
							} finally {
								if (readOptions != EMPTY_READ_OPTIONS) {
									readOptions.close();
								}
							}
							logger.trace(MARKER_ROCKSDB, "Read {}: {}", () -> toStringSafe(key), () -> toStringSafe(result));
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
				.publishOn(dbRScheduler)
				.<ArrayList<Optional<Buffer>>>handle((keysWindow, sink) -> {
					List<Buffer> keyBufsWindow = new ArrayList<>(keysWindow.size());
					for (Send<Buffer> bufferSend : keysWindow) {
						keyBufsWindow.add(bufferSend.receive());
					}
					try {
						assert !Schedulers.isInNonBlockingThread() : "Called getMulti in a nonblocking thread";
						ArrayList<Optional<Buffer>> mappedResults;
						var readOptions = generateReadOptionsOrStatic(snapshot);
						try {
							List<byte[]> results = db.multiGetAsList(readOptions, LLUtils.toArray(keyBufsWindow));
							mappedResults = new ArrayList<>(results.size());
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
						} finally {
							if (readOptions != EMPTY_READ_OPTIONS) {
								readOptions.close();
							}
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
				.publishOn(dbWScheduler)
				.handle((entriesWindowList, sink) -> {
					var entriesWindow = new ArrayList<LLEntry>(entriesWindowList.size());
					for (Send<LLEntry> entrySend : entriesWindowList) {
						entriesWindow.add(entrySend.receive());
					}
					try (var writeOptions = new RocksObj<>(new WriteOptions())) {
						assert !Schedulers.isInNonBlockingThread() : "Called putMulti in a nonblocking thread";
						if (USE_WRITE_BATCHES_IN_PUT_MULTI) {
							try (var batch = new CappedWriteBatch(db,
									alloc,
									CAPPED_WRITE_BATCH_CAP,
									RESERVED_WRITE_BATCH_SIZE,
									MAX_WRITE_BATCH_SIZE,
									writeOptions
							)) {
								for (LLEntry entry : entriesWindow) {
									var k = entry.getKey();
									var v = entry.getValue();
									if (nettyDirect) {
										batch.put(cfh.v(), k, v);
									} else {
										try (var key = k.receive()) {
											try (var value = v.receive()) {
												batch.put(cfh.v(), LLUtils.toArray(key), LLUtils.toArray(value));
											}
										}
									}
								}
								batch.writeToDbAndClose();
							}
						} else {
							for (LLEntry entry : entriesWindow) {
								db.put(writeOptions, entry.getKeyUnsafe(), entry.getValueUnsafe());
							}
						}
						sink.next(true);
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
				.flatMapSequential(ew -> this.<List<Boolean>>runOnDb(true, () -> {
					List<Tuple2<K, Buffer>> entriesWindow = new ArrayList<>(ew.size());
					for (Tuple2<K, Send<Buffer>> tuple : ew) {
						entriesWindow.add(tuple.mapT2(Send::receive));
					}
					try (var writeOptions = new RocksObj<>(new WriteOptions())) {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called updateMulti in a nonblocking thread");
						}
						List<Buffer> keyBufsWindow = new ArrayList<>(entriesWindow.size());
						for (Tuple2<K, Buffer> objects : entriesWindow) {
							keyBufsWindow.add(objects.getT2());
						}
						ArrayList<Tuple3<K, Send<Buffer>, Optional<Send<Buffer>>>> mappedInputs;
						{
							var readOptions = generateReadOptionsOrStatic(null);
							try {
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
							} finally {
								if (readOptions != EMPTY_READ_OPTIONS) {
									readOptions.close();
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
							try (var batch = new CappedWriteBatch(db,
									alloc,
									CAPPED_WRITE_BATCH_CAP,
									RESERVED_WRITE_BATCH_SIZE,
									MAX_WRITE_BATCH_SIZE,
									writeOptions
							)) {
								int i = 0;
								for (Tuple2<K, Buffer> entry : entriesWindow) {
									try (var valueToWrite = updatedValuesToWrite.get(i)) {
										if (valueToWrite == null) {
											batch.delete(cfh.v(), entry.getT2().send());
										} else {
											batch.put(cfh.v(), entry.getT2().send(), valueToWrite.send());
										}
									}
									i++;
								}
								batch.writeToDbAndClose();
							}
						} else {
							int i = 0;
							for (Tuple2<K, Buffer> entry : entriesWindow) {
								db.put(writeOptions, entry.getT2(), updatedValuesToWrite.get(i));
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
			var readOptions = generateReadOptionsOrNull(snapshot);
			return new LLLocalEntryReactiveRocksIterator(db, rangeSend, nettyDirect, readOptions, reverse, smallRange);
		});
		return Flux.usingWhen(iteratorMono,
				iterator -> iterator.flux().subscribeOn(dbRScheduler, false),
				iterator -> Mono.fromRunnable(iterator::close)
		);
	}

	private Flux<List<Send<LLEntry>>> getRangeMultiGrouped(LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono,
			int prefixLength, boolean smallRange) {
		Mono<LLLocalGroupedEntryReactiveRocksIterator> iteratorMono = rangeMono.map(rangeSend -> {
			var readOptions = generateReadOptionsOrNull(snapshot);
			return new LLLocalGroupedEntryReactiveRocksIterator(db,
					prefixLength,
					rangeSend,
					nettyDirect,
					readOptions,
					smallRange
			);
		});
		return Flux.usingWhen(
				iteratorMono,
				iterator -> iterator.flux().subscribeOn(dbRScheduler, false),
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
			var readOptions = generateReadOptionsOrNull(snapshot);
			return new LLLocalGroupedKeyReactiveRocksIterator(db,
					prefixLength,
					rangeSend,
					nettyDirect,
					readOptions,
					smallRange
			);
		});
		return Flux.usingWhen(iteratorMono,
				iterator -> iterator.flux().subscribeOn(dbRScheduler, false),
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
							try (var ro = LLUtils.generateCustomReadOptions(null,
									false,
									isBoundedRange(range),
									false
							)) {
								ro.v().setFillCache(false);
								if (!range.isSingle()) {
									if (LLUtils.MANUAL_READAHEAD) {
										ro.v().setReadaheadSize(32 * 1024);
									}
								}
								ro.v().setVerifyChecksums(true);
								try (var rocksIterator = db.newRocksIterator(nettyDirect, ro, range, false)) {
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
						.subscribeOn(dbRScheduler),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	@Override
	public Flux<Send<Buffer>> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono,
			int prefixLength, boolean smallRange) {
		Mono<LLLocalKeyPrefixReactiveRocksIterator> iteratorMono = rangeMono.map(range -> {
			var readOptions = generateReadOptionsOrNull(snapshot);
			return new LLLocalKeyPrefixReactiveRocksIterator(db,
					prefixLength,
					range,
					nettyDirect,
					readOptions,
					true,
					smallRange
			);
		});
		return Flux.usingWhen(iteratorMono,
				iterator -> iterator.flux().subscribeOn(dbRScheduler),
				iterator -> Mono.fromRunnable(iterator::close)
		);
	}

	private Flux<Send<Buffer>> getRangeKeysSingle(LLSnapshot snapshot, Mono<Send<Buffer>> keyMono) {
		return keyMono
				.publishOn(dbRScheduler)
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

	private record RocksObjTuple<T extends AbstractNativeReference, U extends Resource<?>>(RocksObj<T> t1, U t2) implements SafeCloseable {

		@Override
		public void close() {
			//noinspection EmptyTryBlock
			try (t1; t2) {}
		}
	}

	private Flux<Send<Buffer>> getRangeKeysMulti(LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			boolean reverse,
			boolean smallRange) {
		Mono<RocksObjTuple<ReadOptions, LLLocalKeyReactiveRocksIterator>> iteratorMono = rangeMono.map(range -> {
			var readOptions = generateReadOptionsOrNull(snapshot);
			var it = new LLLocalKeyReactiveRocksIterator(db, range, nettyDirect, readOptions, reverse, smallRange);
			return new RocksObjTuple<>(readOptions, it);
		});
		return Flux.usingWhen(iteratorMono,
				t -> t.t2().flux().subscribeOn(dbRScheduler, false),
				t -> Mono.fromRunnable(t::close)
		);
	}

	@Override
	public Mono<Void> setRange(Mono<Send<LLRange>> rangeMono, Flux<Send<LLEntry>> entries, boolean smallRange) {
		if (USE_WINDOW_IN_SET_RANGE) {
			return rangeMono
					.publishOn(dbWScheduler)
					.<Boolean>handle((rangeSend, sink) -> {
						try (var writeOptions = new RocksObj<>(new WriteOptions());
								var range = rangeSend.receive()) {
							assert !Schedulers.isInNonBlockingThread() : "Called setRange in a nonblocking thread";
							if (!USE_WRITE_BATCH_IN_SET_RANGE_DELETE || !USE_WRITE_BATCHES_IN_SET_RANGE) {
								try (var opts = LLUtils.generateCustomReadOptions(null,
										true,
										isBoundedRange(range),
										smallRange
								)) {
										SafeCloseable seekTo;
										try (var it = db.newIterator(opts, range.getMinUnsafe(), range.getMaxUnsafe())) {
											if (!PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
												it.seekTo(range.getMinUnsafe());
											} else {
												seekTo = null;
												it.seekToFirst();
											}
											while (it.isValid()) {
												db.delete(writeOptions, it.key());
												it.next();
											}
										}
								}
							} else if (USE_CAPPED_WRITE_BATCH_IN_SET_RANGE) {
								try (var batch = new CappedWriteBatch(db,
										alloc,
										CAPPED_WRITE_BATCH_CAP,
										RESERVED_WRITE_BATCH_SIZE,
										MAX_WRITE_BATCH_SIZE,
										writeOptions
								)) {
									if (range.isSingle()) {
										batch.delete(cfh.v(), range.getSingle());
									} else {
										deleteSmallRangeWriteBatch(batch, range.copy().send());
									}
									batch.writeToDbAndClose();
								}
							} else {
								try (var batch = new WriteBatch(RESERVED_WRITE_BATCH_SIZE)) {
									if (range.isSingle()) {
										batch.delete(cfh.v(), LLUtils.toArray(range.getSingleUnsafe()));
									} else {
										deleteSmallRangeWriteBatch(batch, range.copy().send());
									}
									db.write(writeOptions, batch);
									batch.clear();
								}
							}
							sink.next(true);
						} catch (RocksDBException ex) {
							sink.error(new RocksDBException("Failed to set a range: " + ex.getMessage()));
						}
					})
					.thenMany(entries.window(MULTI_GET_WINDOW))
					.flatMap(keysWindowFlux -> keysWindowFlux
							.collectList()
							.flatMap(entriesListSend -> this
									.<Void>runOnDb(true, () -> {
										List<LLEntry> entriesList = new ArrayList<>(entriesListSend.size());
										for (Send<LLEntry> entrySend : entriesListSend) {
											entriesList.add(entrySend.receive());
										}
										try (var writeOptions = new RocksObj<>(new WriteOptions())) {
											if (!USE_WRITE_BATCHES_IN_SET_RANGE) {
												for (LLEntry entry : entriesList) {
													assert entry.isAccessible();
													db.put(writeOptions, entry.getKeyUnsafe(), entry.getValueUnsafe());
												}
											} else if (USE_CAPPED_WRITE_BATCH_IN_SET_RANGE) {
												try (var batch = new CappedWriteBatch(db,
														alloc,
														CAPPED_WRITE_BATCH_CAP,
														RESERVED_WRITE_BATCH_SIZE,
														MAX_WRITE_BATCH_SIZE,
														writeOptions
												)) {
													for (LLEntry entry : entriesList) {
														assert entry.isAccessible();
														if (nettyDirect) {
															batch.put(cfh.v(), entry.getKey(), entry.getValue());
														} else {
															batch.put(cfh.v(),
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
														batch.put(cfh.v(), LLUtils.toArray(entry.getKeyUnsafe()),
																LLUtils.toArray(entry.getValueUnsafe()));
													}
													db.write(writeOptions, batch);
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
					.publishOn(dbWScheduler)
					.handle((oldValueSend, sink) -> {
						try (var writeOptions = new RocksObj<>(new WriteOptions());
								var oldValue = oldValueSend.receive()) {
							db.delete(writeOptions, oldValue.getKeyUnsafe());
							sink.next(true);
						} catch (RocksDBException ex) {
							sink.error(new RocksDBException("Failed to write range: " + ex.getMessage()));
						}
					})
					.then(Mono.<Void>empty());

			var putMono = entries
					.publishOn(dbWScheduler)
					.handle((entrySend, sink) -> {
						try (var entry = entrySend.receive()) {
							if (entry.getKeyUnsafe() != null && entry.getValueUnsafe() != null) {
								this.put(entry.getKeyUnsafe(), entry.getValueUnsafe());
							}
							sink.next(true);
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
		try (var range = rangeToReceive.receive();
				var readOpts = generateReadOptionsOrNew(null)) {
			readOpts.v().setFillCache(false);
			try (var rocksIterator = db.newIterator(readOpts, range.getMinUnsafe(), range.getMaxUnsafe())) {
				if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
					rocksIterator.seekTo(range.getMinUnsafe());
				} else {
					rocksIterator.seekToFirst();
				}
				while (rocksIterator.isValid()) {
					writeBatch.delete(cfh.v(), LLUtils.readDirectNioBuffer(alloc, rocksIterator::key).send());
					rocksIterator.next();
				}
			}
		}
	}

	private void deleteSmallRangeWriteBatch(WriteBatch writeBatch, Send<LLRange> rangeToReceive)
			throws RocksDBException {
		try (var range = rangeToReceive.receive()) {
			try (var readOpts = LLUtils.generateCustomReadOptions(null, true, isBoundedRange(range), true)) {
				readOpts.v().setFillCache(false);
				try (var rocksIterator = db.newIterator(readOpts, range.getMinUnsafe(), range.getMaxUnsafe())) {
					if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
						rocksIterator.seekTo(range.getMinUnsafe());
					} else {
						rocksIterator.seekToFirst();
					}
					while (rocksIterator.isValid()) {
						writeBatch.delete(cfh.v(), rocksIterator.key());
						rocksIterator.next();
					}
				}
			}
		}
	}

	public Mono<Void> clear() {
		return Mono
				.<Void>fromCallable(() -> {
					assert !Schedulers.isInNonBlockingThread() : "Called clear in a nonblocking thread";
					boolean shouldCompactLater = false;
					try (var writeOptions = new RocksObj<>(new WriteOptions());
							var readOpts = LLUtils.generateCustomReadOptions(null, false, false, false)) {
						readOpts.v().setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);

						// readOpts.setIgnoreRangeDeletions(true);
						readOpts.v().setFillCache(false);
						if (LLUtils.MANUAL_READAHEAD) {
							readOpts.v().setReadaheadSize(32 * 1024); // 32KiB
						}
						try (CappedWriteBatch writeBatch = new CappedWriteBatch(db,
								alloc,
								CAPPED_WRITE_BATCH_CAP,
								RESERVED_WRITE_BATCH_SIZE,
								MAX_WRITE_BATCH_SIZE,
								writeOptions
						)) {

							byte[] firstDeletedKey = null;
							byte[] lastDeletedKey = null;
							try (var rocksIterator = db.newIterator(readOpts, null, null)) {
								// If the database supports transactions, delete each key one by one
								if (db.supportsTransactions()) {
									rocksIterator.seekToFirst();
									while (rocksIterator.isValid()) {
										writeBatch.delete(cfh.v(), rocksIterator.key());
										rocksIterator.next();
									}
								} else {
									rocksIterator.seekToLast();

									if (rocksIterator.isValid()) {
										firstDeletedKey = FIRST_KEY;
										lastDeletedKey = rocksIterator.key();
										writeBatch.deleteRange(cfh.v(), FIRST_KEY, rocksIterator.key());
										writeBatch.delete(cfh.v(), rocksIterator.key());
										shouldCompactLater = true;
									}
								}
							}

							writeBatch.writeToDbAndClose();

							if (shouldCompactLater) {
								// Compact range
								db.suggestCompactRange();
								if (lastDeletedKey != null) {
									try (var cro = new RocksObj<>(new CompactRangeOptions()
											.setAllowWriteStall(false)
											.setExclusiveManualCompaction(false)
											.setChangeLevel(false))) {
										db.compactRange(firstDeletedKey, lastDeletedKey, cro);
									}
								}
							}

							try (var fo = new RocksObj<>(new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true))) {
								db.flush(fo);
							}
							db.flushWal(true);
						}
						return null;
					}
				})
				.onErrorMap(cause -> new IOException("Failed to clear", cause))
				.subscribeOn(dbWScheduler);

	}

	@Override
	public Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono, boolean fast) {
		return rangeMono.publishOn(dbRScheduler).handle((rangeSend, sink) -> {
			try (var range = rangeSend.receive()) {
				assert !Schedulers.isInNonBlockingThread() : "Called sizeRange in a nonblocking thread";
				if (range.isAll()) {
					sink.next(fast ? fastSizeAll(snapshot) : exactSizeAll(snapshot));
				} else {
					try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNull(snapshot),
							false,
							isBoundedRange(range),
							false
					)) {
						readOpts.v().setFillCache(false);
						readOpts.v().setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
						if (fast) {
							readOpts.v().setIgnoreRangeDeletions(true);

						}
						try (var rocksIterator = db.newIterator(readOpts, range.getMinUnsafe(), range.getMaxUnsafe())) {
							if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
								rocksIterator.seekTo(range.getMinUnsafe());
							} else {
								rocksIterator.seekToFirst();
							}
							long i = 0;
							while (rocksIterator.isValid()) {
								rocksIterator.next();
								i++;
							}
							sink.next(i);
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
		return rangeMono.publishOn(dbRScheduler).handle((rangeSend, sink) -> {
			try (var range = rangeSend.receive()) {
				assert !Schedulers.isInNonBlockingThread() : "Called getOne in a nonblocking thread";
				try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNull(snapshot), true, true, true)) {
					try (var rocksIterator = db.newIterator(readOpts, range.getMinUnsafe(), range.getMaxUnsafe())) {
						if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
							rocksIterator.seekTo(range.getMinUnsafe());
						} else {
							rocksIterator.seekToFirst();
						}
						if (rocksIterator.isValid()) {
							try (var key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key)) {
								try (var value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value)) {
									sink.next(LLEntry.of(key.send(), value.send()).send());
								}
							}
						} else {
							sink.complete();
						}
					}
				}
			} catch (RocksDBException ex) {
				sink.error(new RocksDBException("Failed to get one entry: " + ex.getMessage()));
			}
		});
	}

	@Override
	public Mono<Send<Buffer>> getOneKey(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return rangeMono.publishOn(dbRScheduler).handle((rangeSend, sink) -> {
			try (var range = rangeSend.receive()) {
				assert !Schedulers.isInNonBlockingThread() : "Called getOneKey in a nonblocking thread";
				try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNull(snapshot), true, true, true)) {
					try (var rocksIterator = db.newIterator(readOpts, range.getMinUnsafe(), range.getMaxUnsafe())) {
						if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
							rocksIterator.seekTo(range.getMinUnsafe());
						} else {
							rocksIterator.seekToFirst();
						}
						if (rocksIterator.isValid()) {
							sink.next(LLUtils.readDirectNioBuffer(alloc, rocksIterator::key).send());
						} else {
							sink.complete();
						}
					}
				}
			} catch (RocksDBException ex) {
				sink.error(new RocksDBException("Failed to get one key: " + ex.getMessage()));
			}
		});
	}

	private long fastSizeAll(@Nullable LLSnapshot snapshot) throws RocksDBException {
		try (var rocksdbSnapshot = generateReadOptionsOrNew(snapshot)) {
			if (USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS || rocksdbSnapshot.v().snapshot() == null) {
				try {
					return db.getLongProperty("rocksdb.estimate-num-keys");
				} catch (RocksDBException e) {
					logger.error(MARKER_ROCKSDB, "Failed to get RocksDB estimated keys count property", e);
					return 0;
				}
			} else if (PARALLEL_EXACT_SIZE) {
				return exactSizeAll(snapshot);
			} else {
				rocksdbSnapshot.v().setFillCache(false);
				rocksdbSnapshot.v().setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
				rocksdbSnapshot.v().setIgnoreRangeDeletions(true);
				long count = 0;
				try (var rocksIterator = db.newIterator(rocksdbSnapshot, null, null)) {
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
		try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNull(snapshot), false, false, false)) {
			if (LLUtils.MANUAL_READAHEAD) {
				readOpts.v().setReadaheadSize(128 * 1024); // 128KiB
			}
			readOpts.v().setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);

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
							try (var rangeReadOpts = new RocksObj<>(new ReadOptions(readOpts.v()))) {
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
										rangeReadOpts.v().setIterateLowerBound(sliceBegin);
									}
									if (sliceEnd != null) {
										rangeReadOpts.v().setIterateUpperBound(sliceEnd);
									}
									try (var rocksIterator = db.newIterator(rangeReadOpts, null, null)) {
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
				try (var rocksIterator = db.newIterator(readOpts, null, null)) {
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
		return rangeMono.publishOn(dbWScheduler).handle((rangeSend, sink) -> {
			try (var range = rangeSend.receive()) {
				assert !Schedulers.isInNonBlockingThread() : "Called removeOne in a nonblocking thread";
				try (var readOpts = new RocksObj<>(new ReadOptions());
						var writeOpts = new RocksObj<>(new WriteOptions())) {
					try (var rocksIterator = db.newIterator(readOpts, range.getMinUnsafe(), range.getMaxUnsafe())) {
						if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
							rocksIterator.seekTo(range.getMinUnsafe());
						} else {
							rocksIterator.seekToFirst();
						}
						if (!rocksIterator.isValid()) {
							sink.complete();
							return;
						}
						Buffer key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
						Buffer value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value);
						db.delete(writeOpts, key);
						sink.next(LLEntry.of(key, value).send());
					}
				}
			} catch (RocksDBException ex) {
				sink.error(ex);
			}
		});
	}

}
