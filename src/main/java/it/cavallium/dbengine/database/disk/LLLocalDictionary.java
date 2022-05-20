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
	private static final ReadOptions EMPTY_READ_OPTIONS = LLUtils.ALLOW_STATIC_OPTIONS ? new ReadOptions() : null;
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
	private final ColumnFamilyHandle cfh;
	private final String databaseName;
	private final String columnName;
	private final Scheduler dbWScheduler;
	private final Scheduler dbRScheduler;
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

	public LLLocalDictionary(BufferAllocator allocator,
			@NotNull RocksDBColumn db,
			String databaseName,
			String columnName,
			Scheduler dbWScheduler,
			Scheduler dbRScheduler,
			Function<LLSnapshot, Snapshot> snapshotResolver,
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
	private ReadOptions generateReadOptionsOrStatic(LLSnapshot snapshot) {
		var resolved = generateReadOptions(snapshot != null ? snapshotResolver.apply(snapshot) : null, true);
		if (resolved != null) {
			return resolved;
		} else {
			return new ReadOptions();
		}
	}

	@Nullable
	private ReadOptions generateReadOptionsOrNull(LLSnapshot snapshot) {
		return generateReadOptions(snapshot != null ? snapshotResolver.apply(snapshot) : null, false);
	}

	@NotNull
	private ReadOptions generateReadOptionsOrNew(LLSnapshot snapshot) {
		var result = generateReadOptions(snapshot != null ? snapshotResolver.apply(snapshot) : null, false);
		if (result != null) {
			return result;
		} else {
			return new ReadOptions();
		}
	}

	private ReadOptions generateReadOptions(Snapshot snapshot, boolean orStaticOpts) {
		if (snapshot != null) {
			return new ReadOptions().setSnapshot(snapshot);
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
	public Mono<Buffer> get(@Nullable LLSnapshot snapshot, Mono<Buffer> keyMono) {
		return keyMono
				.publishOn(dbRScheduler)
				.<Buffer>handle((key, sink) -> {
					try (key) {
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
								sink.next(result);
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
	public Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono, boolean fillCache) {
		return rangeMono.publishOn(dbRScheduler).handle((range, sink) -> {
			try (range) {
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
								readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
								readOpts.setFillCache(fillCache);
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
	public Mono<Buffer> put(Mono<Buffer> keyMono, Mono<Buffer> valueMono,
			LLDictionaryResultType resultType) {
		// Obtain the previous value from the database
		var previousDataMono = this.getPreviousData(keyMono, resultType);
		// Zip the entry to write to the database
		var entryMono = Mono.zip(keyMono, valueMono, (k, v) -> LLEntry.of(
				k.touch("put entry key"),
				v.touch("put entry value")
		));
		// Write the new entry to the database
		Mono<Buffer> putMono = entryMono
				.publishOn(dbWScheduler)
				.handle((entry, sink) -> {
					try {
						try (entry) {
							var key = entry.getKeyUnsafe();
							var value = entry.getValueUnsafe();
							assert key != null : "Key is null";
							assert value != null : "Value is null";
							key.touch("Dictionary put key");
							value.touch("Dictionary put value");
							put(key, value);
						}
						sink.complete();
					} catch (Throwable ex) {
						sink.error(ex);
					}
				});
		// Read the previous data, then write the new data, then return the previous data
		return Flux.concatDelayError(Flux.just(previousDataMono, putMono), true, 1).singleOrEmpty();
	}

	private void put(Buffer key, Buffer value) throws RocksDBException {
		assert key.isAccessible();
		assert value.isAccessible();
		if (logger.isTraceEnabled(MARKER_ROCKSDB)) {
			var varargs = new Supplier<?>[]{() -> toStringSafe(key), () -> toStringSafe(value)};
			logger.trace(MARKER_ROCKSDB, "Writing {}: {}", varargs);
		}
		startedPut.increment();
		try (var writeOptions = new WriteOptions()) {
			putTime.recordCallable(() -> {
				key.touch("low-level put key");
				value.touch("low-level put value");
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
	public Mono<Buffer> update(Mono<Buffer> keyMono,
			BinarySerializationFunction updater,
			UpdateReturnMode updateReturnMode) {
		return keyMono
				.publishOn(dbWScheduler)
				.handle((key, sink) -> {
					try (key) {
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
						try (var writeOptions = new WriteOptions()) {
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
					} catch (Throwable ex) {
						sink.error(ex);
					}
				});
	}

	@SuppressWarnings("DuplicatedCode")
	@Override
	public Mono<LLDelta> updateAndGetDelta(Mono<Buffer> keyMono,
			BinarySerializationFunction updater) {
		return keyMono
				.publishOn(dbWScheduler)
				.handle((key, sink) -> {
					try (key) {
						key.touch("low-level dictionary update");
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
						try (var writeOptions = new WriteOptions()) {
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
					} catch (Throwable ex) {
						sink.error(ex);
					}
				});
	}

	@Override
	public Mono<Buffer> remove(Mono<Buffer> keyMono, LLDictionaryResultType resultType) {
		// Obtain the previous value from the database
		Mono<Buffer> previousDataMono = this.getPreviousData(keyMono, resultType);
		// Delete the value from the database
		Mono<Buffer> removeMono = keyMono
				.publishOn(dbWScheduler)
				.handle((key, sink) -> {
					try (key) {
						logger.trace(MARKER_ROCKSDB, "Deleting {}", () -> toStringSafe(key));
						startedRemove.increment();
						try (var writeOptions = new WriteOptions()) {
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
					} catch (Throwable ex) {
						sink.error(ex);
					}
				});
		// Read the previous data, then delete the data, then return the previous data
		return Flux.concat(previousDataMono, removeMono).singleOrEmpty();
	}

	private Mono<Buffer> getPreviousData(Mono<Buffer> keyMono, LLDictionaryResultType resultType) {
		return switch (resultType) {
			case PREVIOUS_VALUE_EXISTENCE -> keyMono
					.publishOn(dbRScheduler)
					.handle((key, sink) -> {
						try (key) {
							var contained = containsKey(null, key);
							sink.next(LLUtils.booleanToResponseByteBuffer(alloc, contained));
						} catch (Throwable ex) {
							sink.error(ex);
						}
					});
			case PREVIOUS_VALUE -> keyMono
					.publishOn(dbRScheduler)
					.handle((key, sink) -> {
						try (key) {
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
								sink.next(result);
							}
						} catch (Throwable ex) {
							sink.error(ex);
						}
					});
			case VOID -> Mono.empty();
		};
	}

	@Override
	public Flux<Optional<Buffer>> getMulti(@Nullable LLSnapshot snapshot, Flux<Buffer> keys) {
		return keys
				.buffer(MULTI_GET_WINDOW)
				.publishOn(dbRScheduler)
				.<ArrayList<Optional<Buffer>>>handle((keysWindow, sink) -> {
					try {
						assert !Schedulers.isInNonBlockingThread() : "Called getMulti in a nonblocking thread";
						ArrayList<Optional<Buffer>> mappedResults;
						var readOptions = generateReadOptionsOrStatic(snapshot);
						try {
							List<byte[]> results = db.multiGetAsList(readOptions, LLUtils.toArray(keysWindow));
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
						for (Buffer buffer : keysWindow) {
							buffer.close();
						}
					}
				})
				.flatMapIterable(list -> list);
	}

	@Override
	public Mono<Void> putMulti(Flux<LLEntry> entries) {
		return entries
				.buffer(Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.publishOn(dbWScheduler)
				.handle((entriesWindow, sink) -> {
					try (var writeOptions = new WriteOptions()) {
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
									var k = entry.getKeyUnsafe();
									var v = entry.getValueUnsafe();
									if (nettyDirect) {
										batch.put(cfh, k.send(), v.send());
									} else {
										batch.put(cfh, LLUtils.toArray(k), LLUtils.toArray(v));
									}
								}
								batch.flush();
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
	public <K> Flux<Boolean> updateMulti(Flux<K> keys, Flux<Buffer> serializedKeys,
			KVSerializationFunction<K, @Nullable Buffer, @Nullable Buffer> updateFunction) {
		return Flux.zip(keys, serializedKeys)
				.buffer(Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.flatMapSequential(entriesWindow -> this.<List<Boolean>>runOnDb(true, () -> {
					try (var writeOptions = new WriteOptions()) {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called updateMulti in a nonblocking thread");
						}
						List<Buffer> keyBufsWindow = new ArrayList<>(entriesWindow.size());
						for (Tuple2<K, Buffer> objects : entriesWindow) {
							keyBufsWindow.add(objects.getT2());
						}
						ArrayList<Tuple3<K, Buffer, Optional<Buffer>>> mappedInputs;
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
												keyBufsWindow.get(i),
												Optional.of(fromByteArray(alloc, val))
										));
									} else {
										mappedInputs.add(Tuples.of(
												entriesWindow.get(i).getT1(),
												keyBufsWindow.get(i),
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
										try (var t3 = mappedInput.getT3().orElse(null)) {
											valueChangedResult.add(!LLUtils.equals(t3, updatedValue));
										}
										updatedValuesToWrite.add(updatedValue);
									} else {
										try (var t3 = mappedInput.getT3().orElse(null)) {
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
								mappedInput.getT3().ifPresent(Resource::close);
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
											batch.delete(cfh, entry.getT2().send());
										} else {
											batch.put(cfh, entry.getT2().send(), valueToWrite.send());
										}
									}
									i++;
								}
								batch.flush();
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
	public Flux<LLEntry> getRange(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			boolean reverse,
			boolean smallRange) {
		return rangeMono.flatMapMany(range -> {
			try (range) {
				if (range.isSingle()) {
					var rangeSingleMono = rangeMono.map(LLRange::getSingleUnsafe);
					return getRangeSingle(snapshot, rangeSingleMono);
				} else {
					return getRangeMulti(snapshot, rangeMono, reverse, smallRange);
				}
			}
		});
	}

	@Override
	public Flux<List<LLEntry>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			int prefixLength,
			boolean smallRange) {
		return rangeMono.flatMapMany(range -> {
			try (range) {
				if (range.isSingle()) {
					var rangeSingleMono = rangeMono.map(LLRange::getSingleUnsafe);
				
					return getRangeSingle(snapshot, rangeSingleMono).map(List::of);
				} else {
					return getRangeMultiGrouped(snapshot, rangeMono, prefixLength, smallRange);
				}
			}
		});
	}

	@SuppressWarnings("resource")
	private Flux<LLEntry> getRangeSingle(LLSnapshot snapshot, Mono<Buffer> keyMono) {
		return Mono
				.zip(keyMono, this.get(snapshot, keyMono))
				.map(result -> LLEntry.of(
						result.getT1().touch("get-range-single key"),
						result.getT2().touch("get-range-single value")
				))
				.flux();
	}

	private Flux<LLEntry> getRangeMulti(LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			boolean reverse,
			boolean smallRange) {
		Mono<LLLocalEntryReactiveRocksIterator> iteratorMono = rangeMono.map(range -> {
			var readOptions = generateReadOptionsOrNull(snapshot);
			return new LLLocalEntryReactiveRocksIterator(db, range, nettyDirect, readOptions, reverse, smallRange);
		});
		return Flux.usingWhen(iteratorMono,
				iterator -> iterator.flux().subscribeOn(dbRScheduler, false),
				iterator -> Mono.fromRunnable(iterator::close)
		);
	}

	private Flux<List<LLEntry>> getRangeMultiGrouped(LLSnapshot snapshot, Mono<LLRange> rangeMono,
			int prefixLength, boolean smallRange) {
		Mono<LLLocalGroupedEntryReactiveRocksIterator> iteratorMono = rangeMono.map(range -> {
			var readOptions = generateReadOptionsOrNull(snapshot);
			return new LLLocalGroupedEntryReactiveRocksIterator(db,
					prefixLength,
					range,
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
	public Flux<Buffer> getRangeKeys(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			boolean reverse,
			boolean smallRange) {
		return rangeMono.flatMapMany(range -> {
			try (range) {
				if (range.isSingle()) {
					return this.getRangeKeysSingle(snapshot, rangeMono.map(LLRange::getSingleUnsafe));
				} else {
					return this.getRangeKeysMulti(snapshot, rangeMono, reverse, smallRange);
				}
			}
		});
	}

	@Override
	public Flux<List<Buffer>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			int prefixLength,
			boolean smallRange) {
		Mono<LLLocalGroupedKeyReactiveRocksIterator> iteratorMono = rangeMono.map(range -> {
			var readOptions = generateReadOptionsOrNull(snapshot);
			return new LLLocalGroupedKeyReactiveRocksIterator(db,
					prefixLength,
					range,
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
	public Flux<BadBlock> badBlocks(Mono<LLRange> rangeMono) {
		return Flux.usingWhen(rangeMono,
				range -> Flux
						.<BadBlock>create(sink -> {
							try (var ro = LLUtils.generateCustomReadOptions(null,
									false,
									isBoundedRange(range),
									false
							)) {
								ro.setFillCache(false);
								if (!range.isSingle()) {
									if (LLUtils.MANUAL_READAHEAD) {
										ro.setReadaheadSize(32 * 1024);
									}
								}
								ro.setVerifyChecksums(true);
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
				range -> Mono.fromRunnable(range::close)
		);
	}

	@Override
	public Flux<Buffer> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono,
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

	private Flux<Buffer> getRangeKeysSingle(LLSnapshot snapshot, Mono<Buffer> keyMono) {
		return keyMono
				.publishOn(dbRScheduler)
				.<Buffer>handle((key, sink) -> {
					try (key) {
						if (containsKey(snapshot, key)) {
							sink.next(key);
						} else {
							sink.complete();
						}
					} catch (Throwable ex) {
						sink.error(ex);
					}
				})
				.flux();
	}

	private record RocksObjTuple<T extends AbstractNativeReference, U extends Resource<?>>(T t1, U t2) implements SafeCloseable {

		@Override
		public void close() {
			//noinspection EmptyTryBlock
			try (t1; t2) {}
		}
	}

	private Flux<Buffer> getRangeKeysMulti(LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
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
	public Mono<Void> setRange(Mono<LLRange> rangeMono, Flux<LLEntry> entries, boolean smallRange) {
		if (USE_WINDOW_IN_SET_RANGE) {
			return rangeMono
					.publishOn(dbWScheduler)
					.<Boolean>handle((range, sink) -> {
						try (var writeOptions = new WriteOptions(); range) {
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
										batch.delete(cfh, range.getSingle());
									} else {
										deleteSmallRangeWriteBatch(batch, range.copy());
									}
									batch.flush();
								}
							} else {
								try (var batch = new WriteBatch(RESERVED_WRITE_BATCH_SIZE)) {
									if (range.isSingle()) {
										batch.delete(cfh, LLUtils.toArray(range.getSingleUnsafe()));
									} else {
										deleteSmallRangeWriteBatch(batch, range.copy());
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
							.flatMap(entriesList -> this
									.<Void>runOnDb(true, () -> {
										try (var writeOptions = new WriteOptions()) {
											if (!USE_WRITE_BATCHES_IN_SET_RANGE) {
												for (LLEntry entry : entriesList) {
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
														if (nettyDirect) {
															batch.put(cfh, entry.getKeyUnsafe().send(), entry.getValueUnsafe().send());
														} else {
															batch.put(cfh,
																	LLUtils.toArray(entry.getKeyUnsafe()),
																	LLUtils.toArray(entry.getValueUnsafe())
															);
														}
													}
													batch.flush();
												}
											} else {
												try (var batch = new WriteBatch(RESERVED_WRITE_BATCH_SIZE)) {
													for (LLEntry entry : entriesList) {
														batch.put(cfh, LLUtils.toArray(entry.getKeyUnsafe()),
																LLUtils.toArray(entry.getValueUnsafe()));
													}
													db.write(writeOptions, batch);
													batch.clear();
												}
											}
											return null;
										} finally {
											for (LLEntry entry : entriesList) {
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
					.handle((oldValue, sink) -> {
						try (var writeOptions = new WriteOptions(); oldValue) {
							db.delete(writeOptions, oldValue.getKeyUnsafe());
							sink.next(true);
						} catch (RocksDBException ex) {
							sink.error(new RocksDBException("Failed to write range: " + ex.getMessage()));
						}
					})
					.then(Mono.<Void>empty());

			var putMono = entries
					.publishOn(dbWScheduler)
					.handle((entry, sink) -> {
						try (entry) {
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
	private void deleteSmallRangeWriteBatch(CappedWriteBatch writeBatch, LLRange range)
			throws RocksDBException {
		try (range; var readOpts = generateReadOptionsOrNew(null)) {
			readOpts.setFillCache(false);
			try (var rocksIterator = db.newIterator(readOpts, range.getMinUnsafe(), range.getMaxUnsafe())) {
				if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
					rocksIterator.seekTo(range.getMinUnsafe());
				} else {
					rocksIterator.seekToFirst();
				}
				while (rocksIterator.isValid()) {
					writeBatch.delete(cfh, LLUtils.readDirectNioBuffer(alloc, rocksIterator::key).send());
					rocksIterator.next();
				}
			}
		}
	}

	private void deleteSmallRangeWriteBatch(WriteBatch writeBatch, LLRange range)
			throws RocksDBException {
		try (range) {
			try (var readOpts = LLUtils.generateCustomReadOptions(null, true, isBoundedRange(range), true)) {
				readOpts.setFillCache(false);
				try (var rocksIterator = db.newIterator(readOpts, range.getMinUnsafe(), range.getMaxUnsafe())) {
					if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
						rocksIterator.seekTo(range.getMinUnsafe());
					} else {
						rocksIterator.seekToFirst();
					}
					while (rocksIterator.isValid()) {
						writeBatch.delete(cfh, rocksIterator.key());
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
					try (var writeOptions = new WriteOptions();
							var readOpts = LLUtils.generateCustomReadOptions(null, false, false, false)) {
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
								writeOptions
						)) {

							byte[] firstDeletedKey = null;
							byte[] lastDeletedKey = null;
							try (var rocksIterator = db.newIterator(readOpts, null, null)) {
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

							writeBatch.flush();

							if (shouldCompactLater) {
								// Compact range
								db.suggestCompactRange();
								if (lastDeletedKey != null) {
									try (var cro = new CompactRangeOptions()
											.setAllowWriteStall(false)
											.setExclusiveManualCompaction(false)
											.setChangeLevel(false)) {
										db.compactRange(firstDeletedKey, lastDeletedKey, cro);
									}
								}
							}

							try (var fo = new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true)) {
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
	public Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono, boolean fast) {
		return rangeMono.publishOn(dbRScheduler).handle((range, sink) -> {
			try (range) {
				assert !Schedulers.isInNonBlockingThread() : "Called sizeRange in a nonblocking thread";
				if (range.isAll()) {
					sink.next(fast ? fastSizeAll(snapshot) : exactSizeAll(snapshot));
				} else {
					try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNull(snapshot),
							false,
							isBoundedRange(range),
							false
					)) {
						readOpts.setFillCache(false);
						readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
						if (fast) {
							readOpts.setIgnoreRangeDeletions(true);

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
	public Mono<LLEntry> getOne(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return rangeMono.publishOn(dbRScheduler).handle((range, sink) -> {
			try (range) {
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
									sink.next(LLEntry.of(key.touch("get-one key"), value.touch("get-one value")));
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
	public Mono<Buffer> getOneKey(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return rangeMono.publishOn(dbRScheduler).handle((range, sink) -> {
			try (range) {
				assert !Schedulers.isInNonBlockingThread() : "Called getOneKey in a nonblocking thread";
				try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNull(snapshot), true, true, true)) {
					try (var rocksIterator = db.newIterator(readOpts, range.getMinUnsafe(), range.getMaxUnsafe())) {
						if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
							rocksIterator.seekTo(range.getMinUnsafe());
						} else {
							rocksIterator.seekToFirst();
						}
						if (rocksIterator.isValid()) {
							sink.next(LLUtils.readDirectNioBuffer(alloc, rocksIterator::key));
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
									if (sliceEnd != null) {
										rangeReadOpts.setIterateUpperBound(sliceEnd);
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
	public Mono<LLEntry> removeOne(Mono<LLRange> rangeMono) {
		return rangeMono.publishOn(dbWScheduler).handle((range, sink) -> {
			try (range) {
				assert !Schedulers.isInNonBlockingThread() : "Called removeOne in a nonblocking thread";
				try (var readOpts = new ReadOptions();
						var writeOpts = new WriteOptions()) {
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
						sink.next(LLEntry.of(key, value));
					}
				}
			} catch (RocksDBException ex) {
				sink.error(ex);
			}
		});
	}

}
