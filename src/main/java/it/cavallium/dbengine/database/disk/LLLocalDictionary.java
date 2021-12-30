package it.cavallium.dbengine.database.disk;

import static io.net5.buffer.api.StandardAllocationTypes.OFF_HEAP;
import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.asReadOnlyDirect;
import static it.cavallium.dbengine.database.LLUtils.fromByteArray;
import static it.cavallium.dbengine.database.LLUtils.toStringSafe;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import io.net5.util.internal.PlatformDependent;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.Column;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
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
import reactor.util.function.Tuple4;
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
	static final boolean PREFER_SEEK_TO_FIRST = false;
	/**
	 * It used to be false,
	 * now it's true to avoid crashes during iterations on completely corrupted files
	 */
	static final boolean VERIFY_CHECKSUMS_WHEN_NOT_NEEDED = true;
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
	 * Default: true
	 */
	private static final boolean USE_DIRECT_BUFFER_BOUNDS = true;
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
	private final DatabaseOptions databaseOptions;
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
		this.databaseOptions = databaseOptions;
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
	public Mono<Send<Buffer>> get(@Nullable LLSnapshot snapshot,
			Mono<Send<Buffer>> keyMono,
			boolean existsAlmostCertainly) {
		return keyMono
				.publishOn(Schedulers.boundedElastic())
				.<Send<Buffer>>handle((keySend, sink) -> {
					try (var key = keySend.receive()) {
						try {
							var readOptions = requireNonNullElse(resolveSnapshot(snapshot), EMPTY_READ_OPTIONS);
							Buffer result;
							startedGet.increment();
							try {
								result = getTime.recordCallable(() -> db.get(readOptions, key, existsAlmostCertainly));
							} finally {
								endedGet.increment();
							}
							logger.trace(MARKER_ROCKSDB, "Read {}: {}", () -> toStringSafe(key), () -> toStringSafe(result));
							if (result != null) {
								sink.next(result.send());
							} else {
								sink.complete();
							}
						} catch (Exception ex) {
							sink.error(new IOException("Failed to read " + toStringSafe(key), ex));
						}
					}
				})
				.onErrorMap(cause -> new IOException("Failed to read", cause));
	}

	@Override
	public Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return rangeMono
				.publishOn(Schedulers.boundedElastic())
				.<Boolean>handle((rangeSend, sink) -> {
					try (var range = rangeSend.receive()) {
						sink.next(containsRange(snapshot, range));
					} catch (Throwable ex) {
						sink.error(ex);
					}
				})
				.map(isContained -> !isContained);
	}

	public boolean containsRange(@Nullable LLSnapshot snapshot, LLRange range) throws RocksDBException {
		assert !Schedulers.isInNonBlockingThread() : "Called containsRange in a nonblocking thread";
		startedContains.increment();
		try {
			var result = containsTime.recordCallable(() -> {
				if (range.isSingle()) {
					var unmodifiableReadOpts = resolveSnapshot(snapshot);
					return db.exists(unmodifiableReadOpts, range.getSingleUnsafe());
				} else {
					// Temporary resources to release after finished
					AbstractSlice<?> slice1 = null;
					AbstractSlice<?> slice2 = null;
					try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
						readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
						readOpts.setFillCache(false);
						if (range.hasMin()) {
							var rangeMinInternalByteBuffer = asReadOnlyDirect(range.getMinUnsafe());
							if (nettyDirect && rangeMinInternalByteBuffer != null) {
								readOpts.setIterateLowerBound(slice1 = new DirectSlice(rangeMinInternalByteBuffer,
										range.getMinUnsafe().readableBytes()));
							} else {
								readOpts.setIterateLowerBound(slice1 = new Slice(LLUtils.toArray(range.getMinUnsafe())));
							}
						}
						if (range.hasMax()) {
							var rangeMaxInternalByteBuffer = asReadOnlyDirect(range.getMaxUnsafe());
							if (nettyDirect && rangeMaxInternalByteBuffer != null) {
								readOpts.setIterateUpperBound(slice2 = new DirectSlice(rangeMaxInternalByteBuffer,
										range.getMaxUnsafe().readableBytes()));
							} else {
								readOpts.setIterateUpperBound(slice2 = new Slice(LLUtils.toArray(range.getMaxUnsafe())));
							}
						}
						try (RocksIterator rocksIterator = db.newIterator(readOpts)) {
							if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
								var rangeMinInternalByteBuffer = asReadOnlyDirect(range.getMinUnsafe());
								if (nettyDirect && rangeMinInternalByteBuffer != null) {
									rocksIterator.seek(rangeMinInternalByteBuffer);
								} else {
									rocksIterator.seek(LLUtils.toArray(range.getMinUnsafe()));
								}
							} else {
								rocksIterator.seekToFirst();
							}
							rocksIterator.status();
							return rocksIterator.isValid();
						}
					} finally {
						if (slice1 != null) slice1.close();
						if (slice2 != null) slice2.close();
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

	private Mono<Boolean> containsKey(@Nullable LLSnapshot snapshot, Mono<Send<Buffer>> keyMono) {
		return keyMono
				.publishOn(Schedulers.boundedElastic())
				.handle((keySend, sink) -> {
					try (var key = keySend.receive()) {
						sink.next(containsKey(snapshot, key));
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
		var previousDataMono = this.getPreviousData(keyMono, resultType, false);
		// Write the new entry to the database
		var putMono = entryMono
				.publishOn(Schedulers.boundedElastic())
				.<Void>handle((entry, sink) -> {
					try (var key = entry.getKey().receive()) {
						try (var value = entry.getValue().receive()) {
							assert key.isAccessible();
							assert value.isAccessible();
							var varargs = new Supplier<?>[]{() -> toStringSafe(key), () -> toStringSafe(value)};
							logger.trace(MARKER_ROCKSDB, "Writing {}: {}", varargs);
							db.put(EMPTY_WRITE_OPTIONS, key, value);
							sink.complete();
						}
					} catch (Throwable ex) {
						sink.error(ex);
					}
				});
		// Read the previous data, then write the new data, then return the previous data
		return Flux
				.concat(previousDataMono, putMono.then(Mono.empty()))
				.singleOrEmpty()
				// Clean discarded elements
				.doOnDiscard(Send.class, Send::close)
				.doOnDiscard(Entry.class, entry -> {
					if (entry.getKey() instanceof SafeCloseable safeCloseable) {
						safeCloseable.close();
					}
					if (entry.getValue() instanceof SafeCloseable safeCloseable) {
						safeCloseable.close();
					}
				})
				.onErrorMap(cause -> new IOException("Failed to write", cause))
				.elapsed()
				.map(tuple -> {
					putTime.record(tuple.getT1(), TimeUnit.MILLISECONDS);
					return tuple.getT2();
				})
				.doFirst(startedPut::increment)
				.doFinally(s -> endedPut.increment());
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return Mono.fromSupplier(() -> updateMode);
	}

	@SuppressWarnings("DuplicatedCode")
	@Override
	public Mono<Send<Buffer>> update(Mono<Send<Buffer>> keyMono,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono, keySend -> runOnDb(() -> {
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called update in a nonblocking thread");
					}
					if (updateMode == UpdateMode.DISALLOW) {
						throw new UnsupportedOperationException("update() is disallowed");
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
								EMPTY_WRITE_OPTIONS, keySend, updater, existsAlmostCertainly, returnMode));
					} finally {
						endedUpdates.increment();
					}
					assert result != null;
					return switch (updateReturnMode) {
						case NOTHING -> null;
						case GET_NEW_VALUE -> ((UpdateAtomicResultCurrent) result).current();
						case GET_OLD_VALUE -> ((UpdateAtomicResultPrevious) result).previous();
					};
				}).onErrorMap(cause -> new IOException("Failed to read or write", cause)),
				keySend -> Mono.fromRunnable(keySend::close));
	}

	@SuppressWarnings("DuplicatedCode")
	@Override
	public Mono<Send<LLDelta>> updateAndGetDelta(Mono<Send<Buffer>> keyMono,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
			boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono, keySend -> runOnDb(() -> {
					if (Schedulers.isInNonBlockingThread()) {
						keySend.close();
						throw new UnsupportedOperationException("Called update in a nonblocking thread");
					}
					if (updateMode == UpdateMode.DISALLOW) {
						keySend.close();
						throw new UnsupportedOperationException("update() is disallowed");
					}
					if (updateMode == UpdateMode.ALLOW && !db.supportsTransactions()) {
						throw new UnsupportedOperationException("update() is disallowed because the database doesn't support"
								+ "safe atomic operations");
					}
					UpdateAtomicResult result;
					startedUpdates.increment();
					try {
						result = updateTime.recordCallable(() -> db.updateAtomic(EMPTY_READ_OPTIONS,
								EMPTY_WRITE_OPTIONS, keySend, updater, existsAlmostCertainly, UpdateAtomicResultMode.DELTA));
					} finally {
						endedUpdates.increment();
					}
					assert result != null;
					return ((UpdateAtomicResultDelta) result).delta();
				}).onErrorMap(cause -> new IOException("Failed to read or write", cause)),
				keySend -> Mono.fromRunnable(keySend::close)).doOnDiscard(UpdateAtomicResult.class, uar -> {
					if (uar instanceof UpdateAtomicResultDelta delta) {
						delta.delta().close();
					} else if (uar instanceof UpdateAtomicResultCurrent cur) {
						cur.current().close();
					} else if (uar instanceof UpdateAtomicResultPrevious cur) {
						cur.previous().close();
					}
		});
	}

	@Override
	public Mono<Send<Buffer>> remove(Mono<Send<Buffer>> keyMono, LLDictionaryResultType resultType) {
		return Mono.usingWhen(keyMono,
				keySend -> this
						.getPreviousData(keyMono, resultType, true)
						.concatWith(this
								.<Send<Buffer>>runOnDb(() -> {
									try (var key = keySend.receive()) {
										if (logger.isTraceEnabled()) {
											logger.trace(MARKER_ROCKSDB, "Deleting {}", toStringSafe(key));
											db.delete(EMPTY_WRITE_OPTIONS, key);
										} else {
											db.delete(EMPTY_WRITE_OPTIONS, key);
										}
									}
									return null;
								})
								.onErrorMap(cause -> new IOException("Failed to delete", cause))
						)
						.singleOrEmpty(),
				keySend -> Mono.fromRunnable(keySend::close)
		).elapsed().map(tuple -> {
			removeTime.record(tuple.getT1(), TimeUnit.MILLISECONDS);
			return tuple.getT2();
		}).doFirst(startedRemove::increment).doFinally(s -> endedRemove.increment());
	}

	private Mono<Send<Buffer>> getPreviousData(Mono<Send<Buffer>> keyMono, LLDictionaryResultType resultType,
			boolean existsAlmostCertainly) {
		return switch (resultType) {
			case PREVIOUS_VALUE_EXISTENCE -> this
					.containsKey(null, keyMono)
					.single()
					.map((Boolean bool) -> LLUtils.booleanToResponseByteBuffer(alloc, bool));
			case PREVIOUS_VALUE -> Mono.usingWhen(
					keyMono,
					keySend -> this
							.runOnDb(() -> {
								try (var key = keySend.receive()) {
									if (Schedulers.isInNonBlockingThread()) {
										throw new UnsupportedOperationException("Called getPreviousData in a nonblocking thread");
									}

									if (logger.isTraceEnabled()) {
										var keyString = toStringSafe(key);
										var result = db.get(EMPTY_READ_OPTIONS, key, existsAlmostCertainly);
										logger.trace(MARKER_ROCKSDB, "Reading {}: {}", keyString, toStringSafe(result));
										return result == null ? null : result.send();
									} else {
										var result = db.get(EMPTY_READ_OPTIONS, key, existsAlmostCertainly);
										return result == null ? null : result.send();
									}
								}
							})
							.onErrorMap(cause -> new IOException("Failed to read ", cause)),
					keySend -> Mono.fromRunnable(keySend::close));
			case VOID -> Mono.empty();
		};
	}

	@Override
	public Flux<Optional<Buffer>> getMulti(@Nullable LLSnapshot snapshot,
			Flux<Send<Buffer>> keys,
			boolean existsAlmostCertainly) {
		return keys
				.buffer(MULTI_GET_WINDOW)
				.doOnDiscard(Tuple2.class, discardedEntry -> {
					if (discardedEntry.getT2() instanceof Resource<?> resource) {
						resource.close();
					}
				})
				.doOnDiscard(Tuple3.class, discardedEntry -> {
					if (discardedEntry.getT2() instanceof Resource<?> resource) {
						resource.close();
					}
					if (discardedEntry.getT3() instanceof Resource<?> resource) {
						resource.close();
					}
				})
				.flatMapSequential(keysWindow -> runOnDb(() -> {
					List<Buffer> keyBufsWindow = new ArrayList<>(keysWindow.size());
					for (Send<Buffer> bufferSend : keysWindow) {
						keyBufsWindow.add(bufferSend.receive());
					}
					try {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called getMulti in a nonblocking thread");
						}
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
						return mappedResults;
					} finally {
						for (Buffer buffer : keyBufsWindow) {
							buffer.close();
						}
					}
				})
				.flatMapIterable(list -> list)
				.onErrorMap(cause -> new IOException("Failed to read keys", cause))
				.doAfterTerminate(() -> keysWindow.forEach(Send::close)), 2) // Max concurrency is 2 to read data while preparing the next segment
				.doOnDiscard(LLEntry.class, ResourceSupport::close)
				.doOnDiscard(Optional.class, opt -> {
					if (opt.isPresent() && opt.get() instanceof Buffer bb) {
						bb.close();
					}
				});
	}

	@Override
	public Flux<Send<LLEntry>> putMulti(Flux<Send<LLEntry>> entries, boolean getOldValues) {
		return entries
				.buffer(Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.flatMapSequential(ew -> Mono
						.<List<Send<LLEntry>>>fromCallable(() -> {
							var entriesWindow = new ArrayList<LLEntry>(ew.size());
							for (Send<LLEntry> entrySend : ew) {
								entriesWindow.add(entrySend.receive());
							}
							try {
								if (Schedulers.isInNonBlockingThread()) {
									throw new UnsupportedOperationException("Called putMulti in a nonblocking thread");
								}
								ArrayList<Send<LLEntry>> oldValues;
								if (getOldValues) {
									oldValues = new ArrayList<>(entriesWindow.size());
									try (var readOptions = resolveSnapshot(null)) {
										for (LLEntry entry : entriesWindow) {
											try (var key = entry.getKey().receive()) {
												Buffer oldValue = db.get(readOptions, key, false);
												if (oldValue != null) {
													oldValues.add(LLEntry.of(key, oldValue).send());
												}
											}
										}
									}
								} else {
									oldValues = null;
								}
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
								return oldValues;
							} finally {
								for (LLEntry llEntry : entriesWindow) {
									llEntry.close();
								}
							}
						}).subscribeOn(dbScheduler), 2) // Max concurrency is 2 to read data while preparing the next segment
				.flatMapIterable(oldValuesList -> oldValuesList)
				.transform(LLUtils::handleDiscard);
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
				}).flatMapIterable(list -> list), /* Max concurrency is 2 to update data while preparing the next segment */ 2)
				.doOnDiscard(Tuple2.class, entry -> {
					if (entry.getT1() instanceof Buffer bb) {
						bb.close();
					}
					if (entry.getT2() instanceof Buffer bb) {
						bb.close();
					}
				});
	}

	@Override
	public Flux<Send<LLEntry>> getRange(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			boolean existsAlmostCertainly) {
		return Flux.usingWhen(rangeMono,
				rangeSend -> {
					try (var range = rangeSend.receive()) {
						if (range.isSingle()) {
							var rangeSingleMono = rangeMono.map(r -> r.receive().getSingle());
							return getRangeSingle(snapshot, rangeSingleMono, existsAlmostCertainly);
						} else {
							return getRangeMulti(snapshot, rangeMono);
						}
					}
				},
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	@Override
	public Flux<List<Send<LLEntry>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			int prefixLength, boolean existsAlmostCertainly) {
		return Flux.usingWhen(rangeMono,
				rangeSend -> {
					try (var range = rangeSend.receive()) {
						if (range.isSingle()) {
							var rangeSingleMono = rangeMono.map(r -> r.receive().getSingle());
							return getRangeSingle(snapshot, rangeSingleMono, existsAlmostCertainly).map(List::of);
						} else {
							return getRangeMultiGrouped(snapshot, rangeMono, prefixLength);
						}
					}
				},
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	private Flux<Send<LLEntry>> getRangeSingle(LLSnapshot snapshot,
			Mono<Send<Buffer>> keyMono,
			boolean existsAlmostCertainly) {
		return Mono
				.zip(keyMono, this.get(snapshot, keyMono, existsAlmostCertainly))
				.map(result -> LLEntry.of(result.getT1(), result.getT2()).send())
				.flux()
				.transform(LLUtils::handleDiscard);
	}

	private Flux<Send<LLEntry>> getRangeMulti(LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return Flux.usingWhen(rangeMono,
				rangeSend -> Flux.using(
						() -> new LLLocalEntryReactiveRocksIterator(db, rangeSend,
								nettyDirect, resolveSnapshot(snapshot)),
						iterator -> iterator.flux().subscribeOn(dbScheduler, false),
						LLLocalReactiveRocksIterator::close
				).transform(LLUtils::handleDiscard),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	private Flux<List<Send<LLEntry>>> getRangeMultiGrouped(LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono,
			int prefixLength) {
		return Flux.usingWhen(rangeMono,
				rangeSend -> Flux.using(
						() -> new LLLocalGroupedEntryReactiveRocksIterator(db, prefixLength, rangeSend,
								nettyDirect, resolveSnapshot(snapshot)),
						iterator -> iterator.flux().subscribeOn(dbScheduler, false),
						LLLocalGroupedReactiveRocksIterator::close
				).transform(LLUtils::handleDiscard),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	@Override
	public Flux<Send<Buffer>> getRangeKeys(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return Flux.usingWhen(rangeMono,
				rangeSend -> {
					try (var range = rangeSend.receive()) {
						if (range.isSingle()) {
							return this.getRangeKeysSingle(snapshot, rangeMono.map(r -> r.receive().getSingle()));
						} else {
							return this.getRangeKeysMulti(snapshot, rangeMono);
						}
					}
				},
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	@Override
	public Flux<List<Send<Buffer>>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			Mono<Send<LLRange>> rangeMono,
			int prefixLength) {
		return Flux.usingWhen(rangeMono,
				rangeSend -> Flux.using(
						() -> new LLLocalGroupedKeyReactiveRocksIterator(db, prefixLength, rangeSend,
								nettyDirect, resolveSnapshot(snapshot)),
						iterator -> iterator.flux().subscribeOn(dbScheduler, false),
						LLLocalGroupedReactiveRocksIterator::close
				).transform(LLUtils::handleDiscard),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
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
									ro.setReadaheadSize(32 * 1024);
								}
								ro.setVerifyChecksums(true);
								var rocksIteratorTuple = getRocksIterator(alloc,
										nettyDirect, ro, range, db
								);
								try {
									try (var rocksIterator = rocksIteratorTuple.getT1()) {
										rocksIterator.seekToFirst();
										rocksIterator.status();
										while (rocksIterator.isValid() && !sink.isCancelled()) {
											try {
												rocksIterator.status();
												rocksIterator.key(DUMMY_WRITE_ONLY_BYTE_BUFFER);
												rocksIterator.status();
												rocksIterator.value(DUMMY_WRITE_ONLY_BYTE_BUFFER);
												rocksIterator.status();
											} catch (RocksDBException ex) {
												sink.next(new BadBlock(databaseName, Column.special(columnName), null, ex));
											}
											rocksIterator.next();
										}
									}
								} finally {
									rocksIteratorTuple.getT2().close();
									rocksIteratorTuple.getT3().close();
									rocksIteratorTuple.getT4().close();
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
			int prefixLength) {
		return Flux.usingWhen(rangeMono,
				rangeSend -> Flux
						.using(
								() -> new LLLocalKeyPrefixReactiveRocksIterator(db,
										prefixLength,
										rangeSend,
										nettyDirect,
										resolveSnapshot(snapshot),
										true
								),
								LLLocalKeyPrefixReactiveRocksIterator::flux,
								LLLocalKeyPrefixReactiveRocksIterator::close
						)
						.subscribeOn(dbScheduler),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	private Flux<Send<Buffer>> getRangeKeysSingle(LLSnapshot snapshot, Mono<Send<Buffer>> keyMono) {
		return Flux.usingWhen(keyMono,
				keySend -> this
						.containsKey(snapshot, keyMono)
						.<Send<Buffer>>handle((contains, sink) -> {
							if (contains) {
								sink.next(keySend);
							} else {
								sink.complete();
							}
						})
						.flux()
						.doOnDiscard(Buffer.class, Buffer::close),
				keySend -> Mono.fromRunnable(keySend::close)
		);
	}

	private Flux<Send<Buffer>> getRangeKeysMulti(LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return Flux.usingWhen(rangeMono,
				rangeSend -> Flux.using(
						() -> new LLLocalKeyReactiveRocksIterator(db, rangeSend,
								nettyDirect, resolveSnapshot(snapshot)
						),
						iterator -> iterator.flux().subscribeOn(dbScheduler, false),
						LLLocalReactiveRocksIterator::close
				).transform(LLUtils::handleDiscard),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	@Override
	public Mono<Void> setRange(Mono<Send<LLRange>> rangeMono, Flux<Send<LLEntry>> entries) {
		return Mono.usingWhen(rangeMono,
				rangeSend -> {
					if (USE_WINDOW_IN_SET_RANGE) {
						return this
								.<Void>runOnDb(() -> {
									try (var range = rangeSend.receive()) {
										if (Schedulers.isInNonBlockingThread()) {
											throw new UnsupportedOperationException("Called setRange in a nonblocking thread");
										}
										if (!USE_WRITE_BATCH_IN_SET_RANGE_DELETE || !USE_WRITE_BATCHES_IN_SET_RANGE) {
											assert EMPTY_READ_OPTIONS.isOwningHandle();
											try (var opts = new ReadOptions(EMPTY_READ_OPTIONS)) {
												ReleasableSlice minBound;
												if (range.hasMin()) {
													minBound = setIterateBound(nettyDirect, opts, IterateBound.LOWER, range.getMinUnsafe());
												} else {
													minBound = emptyReleasableSlice();
												}
												try {
													ReleasableSlice maxBound;
													if (range.hasMax()) {
														maxBound = setIterateBound(nettyDirect, opts, IterateBound.UPPER, range.getMaxUnsafe());
													} else {
														maxBound = emptyReleasableSlice();
													}
													assert cfh.isOwningHandle();
													assert opts.isOwningHandle();
													SafeCloseable seekTo;
													try (RocksIterator it = db.newIterator(opts)) {
														if (!PREFER_SEEK_TO_FIRST && range.hasMin()) {
															seekTo = rocksIterSeekTo(nettyDirect, it, range.getMinUnsafe());
														} else {
															seekTo = null;
															it.seekToFirst();
														}
														try {
															it.status();
															while (it.isValid()) {
																db.delete(EMPTY_WRITE_OPTIONS, it.key());
																it.next();
																it.status();
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
										return null;
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
							return Mono.fromCallable(() -> {
								throw new UnsupportedOperationException("Can't use write batches in setRange without window."
										+ " Please fix the parameters");
							});
						}
						return this
								.getRange(null, rangeMono, false)
								.flatMap(oldValueSend -> this.<Void>runOnDb(() -> {
									try (var oldValue = oldValueSend.receive()) {
										db.delete(EMPTY_WRITE_OPTIONS, oldValue.getKeyUnsafe());
										return null;
									}
								}))
								.then(entries
										.flatMap(entrySend -> Mono.using(
												entrySend::receive,
												entry -> this
														.put(LLUtils.lazyRetain(entry::getKey), LLUtils.lazyRetain(entry::getValue),
																LLDictionaryResultType.VOID)
														.doOnNext(Send::close),
												ResourceSupport::close
										))
										.then(Mono.<Void>empty())
								)
								.onErrorMap(cause -> new IOException("Failed to write range", cause));
					}
				},
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	//todo: this is broken, check why. (is this still true?)
	private void deleteSmallRangeWriteBatch(CappedWriteBatch writeBatch, Send<LLRange> rangeToReceive)
			throws RocksDBException {
		var range = rangeToReceive.receive();
		try (var readOpts = new ReadOptions(getReadOptions(null))) {
			readOpts.setFillCache(false);
			ReleasableSlice minBound;
			if (range.hasMin()) {
				minBound = setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
			} else {
				minBound = emptyReleasableSlice();
			}
			try {
				ReleasableSlice maxBound;
				if (range.hasMax()) {
					maxBound = setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
				} else {
					maxBound = emptyReleasableSlice();
				}
				try (var rocksIterator = db.newIterator(readOpts)) {
					SafeCloseable seekTo;
					if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
						seekTo = rocksIterSeekTo(nettyDirect, rocksIterator, range.getMinUnsafe());
					} else {
						seekTo = null;
						rocksIterator.seekToFirst();
					}
					try {
						rocksIterator.status();
						while (rocksIterator.isValid()) {
							writeBatch.delete(cfh, LLUtils.readDirectNioBuffer(alloc, rocksIterator::key).send());
							rocksIterator.next();
							rocksIterator.status();
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
					minBound = setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
				} else {
					minBound = emptyReleasableSlice();
				}
				try {
					ReleasableSlice maxBound;
					if (range.hasMax()) {
						maxBound = setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
					} else {
						maxBound = emptyReleasableSlice();
					}
					try (var rocksIterator = db.newIterator(readOpts)) {
						SafeCloseable seekTo;
						if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
							seekTo = rocksIterSeekTo(nettyDirect, rocksIterator, range.getMinUnsafe());
						} else {
							seekTo = null;
							rocksIterator.seekToFirst();
						}
						try {
							rocksIterator.status();
							while (rocksIterator.isValid()) {
								writeBatch.delete(cfh, rocksIterator.key());
								rocksIterator.next();
								rocksIterator.status();
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

	@Nullable
	private static SafeCloseable rocksIterSeekTo(boolean allowNettyDirect,
			RocksIterator rocksIterator, Buffer key) {
		ByteBuffer keyInternalByteBuffer;
		if (allowNettyDirect && (keyInternalByteBuffer = asReadOnlyDirect(key)) != null) {
			assert keyInternalByteBuffer.position() == 0;
			rocksIterator.seek(keyInternalByteBuffer);
			// This is useful to retain the key buffer in memory and avoid deallocations
			return key::isAccessible;
		} else {
			rocksIterator.seek(LLUtils.toArray(key));
			return null;
		}
	}

	private static ReleasableSlice setIterateBound(boolean allowNettyDirect,
			ReadOptions readOpts, IterateBound boundType, Buffer key) {
		requireNonNull(key);
		AbstractSlice<?> slice;
		ByteBuffer keyInternalByteBuffer;
		if (allowNettyDirect && LLLocalDictionary.USE_DIRECT_BUFFER_BOUNDS
				&& (keyInternalByteBuffer = asReadOnlyDirect(key)) != null) {
			assert keyInternalByteBuffer.position() == 0;
			slice = new DirectSlice(keyInternalByteBuffer, key.readableBytes());
			assert slice.size() == key.readableBytes();
			assert slice.compare(new Slice(LLUtils.toArray(key))) == 0;
			if (boundType == IterateBound.LOWER) {
				readOpts.setIterateLowerBound(slice);
			} else {
				readOpts.setIterateUpperBound(slice);
			}
			return new ReleasableSliceImpl(slice, null, key);
		} else {
			slice = new Slice(requireNonNull(LLUtils.toArray(key)));
			if (boundType == IterateBound.LOWER) {
				readOpts.setIterateLowerBound(slice);
			} else {
				readOpts.setIterateUpperBound(slice);
			}
			return new ReleasableSliceImpl(slice, null, null);
		}
	}

	private static ReleasableSlice emptyReleasableSlice() {
		var arr = new byte[0];

		return new SimpleSliceWithoutRelease(new Slice(arr), null, arr);
	}

	public record SimpleSliceWithoutRelease(AbstractSlice<?> slice, @Nullable Buffer byteBuf,
																								 @Nullable Object additionalData) implements ReleasableSlice {}

	public record ReleasableSliceImpl(AbstractSlice<?> slice, @Nullable Buffer byteBuf,
																					 @Nullable Object additionalData) implements ReleasableSlice {

		@Override
		public void close() {
			slice.clear();
			slice.close();
			if (byteBuf != null) {
				byteBuf.close();
			}
			if (additionalData instanceof ByteBuffer bb && bb.isDirect()) {
				PlatformDependent.freeDirectBuffer(bb);
			}
		}
	}

	public Mono<Void> clear() {
		return Mono
				.<Void>fromCallable(() -> {
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called clear in a nonblocking thread");
					}
					boolean shouldCompactLater = false;
					try (var readOpts = new ReadOptions(getReadOptions(null))) {
						readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);

						// readOpts.setIgnoreRangeDeletions(true);
						readOpts.setFillCache(false);
						readOpts.setReadaheadSize(32 * 1024); // 32KiB
						try (CappedWriteBatch writeBatch = new CappedWriteBatch(db,
								alloc,
								CAPPED_WRITE_BATCH_CAP,
								RESERVED_WRITE_BATCH_SIZE,
								MAX_WRITE_BATCH_SIZE,
								BATCH_WRITE_OPTIONS
						)) {

							byte[] firstDeletedKey = null;
							byte[] lastDeletedKey = null;
							try (RocksIterator rocksIterator = db.newIterator(readOpts)) {
								// If the database supports transactions, delete each key one by one
								if (db.supportsTransactions()) {
									rocksIterator.seekToFirst();
									rocksIterator.status();
									while (rocksIterator.isValid()) {
										writeBatch.delete(cfh, rocksIterator.key());
										rocksIterator.next();
										rocksIterator.status();
									}
								} else {
									rocksIterator.seekToLast();

									rocksIterator.status();
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
		return Mono.usingWhen(rangeMono, rangeSend -> runOnDb(() -> {
			try (var range = rangeSend.receive()) {
				if (Schedulers.isInNonBlockingThread()) {
					throw new UnsupportedOperationException("Called sizeRange in a nonblocking thread");
				}
				if (range.isAll()) {
					return fast ? fastSizeAll(snapshot) : exactSizeAll(snapshot);
				} else {
					try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
						readOpts.setFillCache(false);
						readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
						ReleasableSlice minBound;
						if (range.hasMin()) {
							minBound = setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
						} else {
							minBound = emptyReleasableSlice();
						}
						try {
							ReleasableSlice maxBound;
							if (range.hasMax()) {
								maxBound = setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
							} else {
								maxBound = emptyReleasableSlice();
							}
							try {
								if (fast) {
									readOpts.setIgnoreRangeDeletions(true);

								}
								try (var rocksIterator = db.newIterator(readOpts)) {
									SafeCloseable seekTo;
									if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
										seekTo = rocksIterSeekTo(nettyDirect, rocksIterator, range.getMinUnsafe());
									} else {
										seekTo = null;
										rocksIterator.seekToFirst();
									}
									try {
										long i = 0;
										rocksIterator.status();
										while (rocksIterator.isValid()) {
											rocksIterator.next();
											rocksIterator.status();
											i++;
										}
										return i;
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
			}
		}).onErrorMap(cause -> new IOException("Failed to get size of range", cause)),
				rangeSend -> Mono.fromRunnable(rangeSend::close));
	}

	@Override
	public Mono<Send<LLEntry>> getOne(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return Mono.usingWhen(rangeMono,
				rangeSend -> runOnDb(() -> {
					try (var range = rangeSend.receive()) {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called getOne in a nonblocking thread");
						}
						try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
							ReleasableSlice minBound;
							if (range.hasMin()) {
								minBound = setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
							} else {
								minBound = emptyReleasableSlice();
							}
							try {
								ReleasableSlice maxBound;
								if (range.hasMax()) {
									maxBound = setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
								} else {
									maxBound = emptyReleasableSlice();
								}
								try (var rocksIterator = db.newIterator(readOpts)) {
									SafeCloseable seekTo;
									if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
										seekTo = rocksIterSeekTo(nettyDirect, rocksIterator, range.getMinUnsafe());
									} else {
										seekTo = null;
										rocksIterator.seekToFirst();
									}
									try {
										rocksIterator.status();
										if (rocksIterator.isValid()) {
											try (var key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key)) {
												try (var value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value)) {
													return LLEntry.of(key.send(), value.send()).send();
												}
											}
										} else {
											return null;
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
				}),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	@Override
	public Mono<Send<Buffer>> getOneKey(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return Mono.usingWhen(rangeMono,
				rangeSend -> runOnDb(() -> {
					try (var range = rangeSend.receive()) {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called getOneKey in a nonblocking thread");
						}
						try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
							ReleasableSlice minBound;
							if (range.hasMin()) {
								minBound = setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
							} else {
								minBound = emptyReleasableSlice();
							}
							try {
								ReleasableSlice maxBound;
								if (range.hasMax()) {
									maxBound = setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
								} else {
									maxBound = emptyReleasableSlice();
								}
								try (var rocksIterator = db.newIterator(readOpts)) {
									SafeCloseable seekTo;
									if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
										seekTo = rocksIterSeekTo(nettyDirect, rocksIterator, range.getMinUnsafe());
									} else {
										seekTo = null;
										rocksIterator.seekToFirst();
									}
									try {
										rocksIterator.status();
										if (rocksIterator.isValid()) {
											return LLUtils.readDirectNioBuffer(alloc, rocksIterator::key).send();
										} else {
											return null;
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
				}),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
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
				try (RocksIterator rocksIterator = db.newIterator(rocksdbSnapshot)) {
					rocksIterator.seekToFirst();
					rocksIterator.status();
					// If it's a fast size of a snapshot, count only up to 100'000 elements
					while (rocksIterator.isValid() && count < 100_000) {
						count++;
						rocksIterator.next();
						rocksIterator.status();
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
			readOpts.setReadaheadSize(32 * 1024); // 32KiB
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
									try (RocksIterator rocksIterator = db.newIterator(rangeReadOpts)) {
										rocksIterator.seekToFirst();
										rocksIterator.status();
										while (rocksIterator.isValid()) {
											partialCount++;
											rocksIterator.next();
											rocksIterator.status();
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
				try (RocksIterator iter = db.newIterator(readOpts)) {
					iter.seekToFirst();
					while (iter.isValid()) {
						count++;
						iter.next();
					}
					return count;
				}
			}
		}
	}

	@Override
	public Mono<Send<LLEntry>> removeOne(Mono<Send<LLRange>> rangeMono) {
		return Mono.usingWhen(rangeMono,
				rangeSend -> runOnDb(() -> {
					try (var range = rangeSend.receive()) {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called removeOne in a nonblocking thread");
						}
						try (var readOpts = new ReadOptions(getReadOptions(null))) {
							ReleasableSlice minBound;
							if (range.hasMin()) {
								minBound = setIterateBound(nettyDirect, readOpts, IterateBound.LOWER, range.getMinUnsafe());
							} else {
								minBound = emptyReleasableSlice();
							}
							try {
								ReleasableSlice maxBound;
								if (range.hasMax()) {
									maxBound = setIterateBound(nettyDirect, readOpts, IterateBound.UPPER, range.getMaxUnsafe());
								} else {
									maxBound = emptyReleasableSlice();
								}
								try (RocksIterator rocksIterator = db.newIterator(readOpts)) {
									SafeCloseable seekTo;
									if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
										seekTo = rocksIterSeekTo(nettyDirect, rocksIterator, range.getMinUnsafe());
									} else {
										seekTo = null;
										rocksIterator.seekToFirst();
									}
									try {
										rocksIterator.status();
										if (!rocksIterator.isValid()) {
											return null;
										}
										Buffer key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
										Buffer value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value);
										db.delete(EMPTY_WRITE_OPTIONS, key);
										return LLEntry.of(key, value).send();
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
				}).onErrorMap(cause -> new IOException("Failed to delete", cause)),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	@NotNull
	public static Tuple4<RocksIterator, ReleasableSlice, ReleasableSlice, SafeCloseable> getRocksIterator(BufferAllocator alloc,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			LLRange range,
			RocksDBColumn db) {
		if (Schedulers.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called getRocksIterator in a nonblocking thread");
		}
		ReleasableSlice sliceMin;
		ReleasableSlice sliceMax;
		if (range.hasMin()) {
			sliceMin = setIterateBound(allowNettyDirect, readOptions, IterateBound.LOWER, range.getMinUnsafe());
		} else {
			sliceMin = emptyReleasableSlice();
		}
		if (range.hasMax()) {
			sliceMax = setIterateBound(allowNettyDirect, readOptions, IterateBound.UPPER, range.getMaxUnsafe());
		} else {
			sliceMax = emptyReleasableSlice();
		}
		var rocksIterator = db.newIterator(readOptions);
		SafeCloseable seekTo;
		if (!PREFER_SEEK_TO_FIRST && range.hasMin()) {
			seekTo = Objects.requireNonNullElseGet(rocksIterSeekTo(allowNettyDirect, rocksIterator, range.getMinUnsafe()),
					() -> ((SafeCloseable) () -> {}));
		} else {
			seekTo = () -> {};
			rocksIterator.seekToFirst();
		}
		return Tuples.of(rocksIterator, sliceMin, sliceMax, seekTo);
	}
}
