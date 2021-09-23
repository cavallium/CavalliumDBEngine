package it.cavallium.dbengine.database.disk;

import static io.net5.buffer.Unpooled.wrappedBuffer;
import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.fromByteArray;
import static java.util.Objects.requireNonNull;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import io.net5.util.internal.PlatformDependent;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.ExtraKeyOperationResult;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.RepeatedElementList;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.BiSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractSlice;
import org.rocksdb.CappedWriteBatch;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.DirectSlice;
import org.rocksdb.FlushOptions;
import org.rocksdb.Holder;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;
import org.warp.commonutils.locks.Striped;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

@NotAtomic
public class LLLocalDictionary implements LLDictionary {

	protected static final Logger logger = LoggerFactory.getLogger(LLLocalDictionary.class);
	private static final boolean USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS = false;
	static final int RESERVED_WRITE_BATCH_SIZE = 2 * 1024 * 1024; // 2MiB
	static final long MAX_WRITE_BATCH_SIZE = 1024L * 1024L * 1024L; // 1GiB
	static final int CAPPED_WRITE_BATCH_CAP = 50000; // 50K operations
	static final int MULTI_GET_WINDOW = 16;
	static final Duration MULTI_GET_WINDOW_TIMEOUT = Duration.ofSeconds(1);
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

	private static final int STRIPES = 512;
	private static final byte[] FIRST_KEY = new byte[]{};
	private static final byte[] NO_DATA = new byte[0];

	private static final boolean ASSERTIONS_ENABLED;
	/**
	 * Default: true
	 */
	private static final boolean USE_DIRECT_BUFFER_BOUNDS = true;
	private static final int INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES = 4096;

	/**
	 * 1KiB dummy buffer, write only, used for debugging purposes
	 */
	private static final ByteBuffer DUMMY_WRITE_ONLY_BYTE_BUFFER = ByteBuffer.allocateDirect(1024);

	static {
		boolean assertionsEnabled = false;
		//noinspection AssertWithSideEffects
		assert (assertionsEnabled = true);
		//noinspection ConstantConditions
		ASSERTIONS_ENABLED = assertionsEnabled;
	}

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final String databaseName;
	private final String columnName;
	private final Scheduler dbScheduler;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;
	private final Striped<StampedLock> itemsLock = Striped.readWriteStampedLock(STRIPES);
	private final UpdateMode updateMode;
	private final BufferAllocator alloc;
	private final String getRangeMultiDebugName;
	private final String getRangeKeysMultiDebugName;
	private final DatabaseOptions databaseOptions;

	public LLLocalDictionary(
			BufferAllocator allocator,
			@NotNull RocksDB db,
			@NotNull ColumnFamilyHandle columnFamilyHandle,
			String databaseName,
			String columnName,
			Scheduler dbScheduler,
			Function<LLSnapshot, Snapshot> snapshotResolver,
			UpdateMode updateMode,
			DatabaseOptions databaseOptions) {
		requireNonNull(db);
		this.db = db;
		requireNonNull(columnFamilyHandle);
		this.cfh = columnFamilyHandle;
		this.databaseName = databaseName;
		this.columnName = columnName;
		this.dbScheduler = dbScheduler;
		this.snapshotResolver = snapshotResolver;
		this.updateMode = updateMode;
		this.getRangeMultiDebugName = databaseName + "(" + columnName + ")" + "::getRangeMulti";
		this.getRangeKeysMultiDebugName = databaseName + "(" + columnName + ")" + "::getRangeKeysMulti";
		this.databaseOptions = databaseOptions;
		alloc = allocator;
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

	private int getLockIndex(Buffer key) {
		return Math.abs(LLUtils.hashCode(key) % STRIPES);
	}

	private IntArrayList getLockIndices(List<Buffer> keys) {
		var list = new IntArrayList(keys.size());
		for (Buffer key : keys) {
			list.add(getLockIndex(key));
		}
		return list;
	}

	private IntArrayList getLockIndicesEntries(List<LLEntry> keys) {
		var list = new IntArrayList(keys.size());
		for (LLEntry key : keys) {
			list.add(getLockIndex(key.getKeyUnsafe()));
		}
		return list;
	}

	private <X> IntArrayList getLockIndicesWithExtra(List<Tuple2<Buffer, X>> entries) {
		var list = new IntArrayList(entries.size());
		for (Tuple2<Buffer, X> key : entries) {
			list.add(getLockIndex(key.getT1()));
		}
		return list;
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
		return Mono.usingWhen(keyMono,
				keySend -> runOnDb(() -> {
					try (var key = keySend.receive()) {
						try {
							StampedLock lock;
							long stamp;
							if (updateMode == UpdateMode.ALLOW) {
								lock = itemsLock.getAt(getLockIndex(key));

								stamp = lock.readLock();
							} else {
								lock = null;
								stamp = 0;
							}
							try {
								Buffer logKey;
								if (logger.isTraceEnabled(MARKER_ROCKSDB)) {
									logKey = key.copy();
								} else {
									logKey = null;
								}
								try (logKey) {
									var result = dbGet(cfh, resolveSnapshot(snapshot), key.send(), existsAlmostCertainly);
									if (logger.isTraceEnabled(MARKER_ROCKSDB)) {
										try (var result2 = result == null ? null : result.receive()) {
											logger.trace(MARKER_ROCKSDB, "Reading {}: {}", LLUtils.toStringSafe(logKey), LLUtils.toString(result2));
											return result2 == null ? null : result2.send();
										}
									} else {
										return result;
									}
								}
							} finally {
								if (updateMode == UpdateMode.ALLOW) {
									lock.unlockRead(stamp);
								}
							}
						} catch (Exception ex) {
							throw new IOException("Failed to read " + LLUtils.toStringSafe(key), ex);
						}
					}
				}).onErrorMap(cause -> new IOException("Failed to read", cause)),
				keySend -> Mono.fromRunnable(keySend::close)
		);
	}

	@Nullable
	private Send<Buffer> dbGet(ColumnFamilyHandle cfh,
			@Nullable ReadOptions readOptions,
			Send<Buffer> keySend,
			boolean existsAlmostCertainly) throws RocksDBException {
		try (var key = keySend.receive()) {
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called dbGet in a nonblocking thread");
			}
			if (databaseOptions.allowNettyDirect()) {

				//todo: implement keyMayExist if existsAlmostCertainly is false.
				// Unfortunately it's not feasible until RocksDB implements keyMayExist with buffers

				// Create the key nio buffer to pass to RocksDB
				var keyNioBuffer = LLUtils.convertToReadableDirect(alloc, key.send());
				// Create a direct result buffer because RocksDB works only with direct buffers
				try (Buffer resultBuf = alloc.allocate(INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES)) {
					int valueSize;
					int assertionReadData = -1;
					ByteBuffer resultNioBuf;
					do {
						// Create the result nio buffer to pass to RocksDB
						resultNioBuf = LLUtils.obtainDirect(resultBuf, true);
						assert keyNioBuffer.byteBuffer().isDirect();
						assert resultNioBuf.isDirect();
						valueSize = db.get(cfh,
								Objects.requireNonNullElse(readOptions, EMPTY_READ_OPTIONS),
								keyNioBuffer.byteBuffer().position(0),
								resultNioBuf
						);
						if (valueSize != RocksDB.NOT_FOUND) {
							if (ASSERTIONS_ENABLED) {
								// todo: check if position is equal to data that have been read
								// todo: check if limit is equal to value size or data that have been read
								assert valueSize <= 0 || resultNioBuf.limit() > 0;

								// If the locking is enabled the data is safe, so since we are appending data
								// to the end, we need to check if it has been appended correctly or it
								// has been overwritten.
								// We must not do this check otherwise because if there is no locking the data
								// can be overwritten with a smaller value the next time.
								if (updateMode == UpdateMode.ALLOW) {
									// Check if read data is larger than previously read data.
									// If it's smaller or equals it means that RocksDB is overwriting
									// the beginning of the result buffer.
									assert resultNioBuf.limit() > assertionReadData;
									//noinspection ConstantConditions
									if (ASSERTIONS_ENABLED) {
										assertionReadData = resultNioBuf.limit();
									}
								}

								// Check if read data is not bigger than the total value size.
								// If it's bigger it means that RocksDB is writing the start
								// of the result into the result buffer more than once.
								assert resultNioBuf.limit() <= valueSize;
							}

							if (valueSize <= resultNioBuf.limit()) {
								// Return the result ready to be read
								return resultBuf.readerOffset(0).writerOffset(valueSize).send();
							} else {
								// If the locking is enabled the data is safe, so we can append the next read data.
								// Otherwise we need to re-read everything.
								if (updateMode == UpdateMode.ALLOW) {
									// Update the resultBuf writerIndex with the new position
									resultBuf.writerOffset(resultNioBuf.limit());
								}
								//noinspection UnusedAssignment
								resultNioBuf = null;
							}
							// Rewind the keyNioBuf position, making it readable again for the next loop iteration
							keyNioBuffer.byteBuffer().rewind();
							if (resultBuf.capacity() < valueSize) {
								// Expand the resultBuf size if the result is bigger than the current result
								// buffer size
								resultBuf.ensureWritable(valueSize);
							}
						}
						// Repeat if the result has been found but it's still not finished
					} while (valueSize != RocksDB.NOT_FOUND);
					// If the value is not found return null
					return null;
				} finally {
					keyNioBuffer.buffer().close();
					PlatformDependent.freeDirectBuffer(keyNioBuffer.byteBuffer());
				}
			} else {
				ReadOptions validReadOptions = Objects.requireNonNullElse(readOptions, EMPTY_READ_OPTIONS);
				try {
					byte[] keyArray = LLUtils.toArray(key);
					requireNonNull(keyArray);
					Holder<byte[]> data = existsAlmostCertainly ? null : new Holder<>();
					if (existsAlmostCertainly || db.keyMayExist(cfh, validReadOptions, keyArray, data)) {
						if (!existsAlmostCertainly && data.getValue() != null) {
							return LLUtils.fromByteArray(alloc, data.getValue()).send();
						} else {
							byte[] result = db.get(cfh, validReadOptions, keyArray);
							if (result == null) {
								return null;
							} else {
								return LLUtils.fromByteArray(alloc, result).send();
							}
						}
					} else {
						return null;
					}
				} finally {
					if (!(validReadOptions instanceof UnreleasableReadOptions)) {
						validReadOptions.close();
					}
				}
			}
		}
	}

	@SuppressWarnings("SameParameterValue")
	private void dbPut(ColumnFamilyHandle cfh,
			@Nullable WriteOptions writeOptions,
			Send<Buffer> keyToReceive,
			Send<Buffer> valueToReceive) throws RocksDBException {
		WriteOptions validWriteOptions = Objects.requireNonNullElse(writeOptions, EMPTY_WRITE_OPTIONS);
		try {
			try (var key = keyToReceive.receive()) {
				try (var value = valueToReceive.receive()) {
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called dbPut in a nonblocking thread");
					}
					assert key.isAccessible();
					assert value.isAccessible();
					if (databaseOptions.allowNettyDirect()) {
						var keyNioBuffer = LLUtils.convertToReadableDirect(alloc, key.send());
						try (var ignored1 = keyNioBuffer.buffer().receive()) {
							assert keyNioBuffer.byteBuffer().isDirect();
							var valueNioBuffer = LLUtils.convertToReadableDirect(alloc, value.send());
							try (var ignored2 = valueNioBuffer.buffer().receive()) {
								assert valueNioBuffer.byteBuffer().isDirect();
								db.put(cfh, validWriteOptions, keyNioBuffer.byteBuffer(), valueNioBuffer.byteBuffer());
							} finally {
								PlatformDependent.freeDirectBuffer(valueNioBuffer.byteBuffer());
							}
						} finally {
							PlatformDependent.freeDirectBuffer(keyNioBuffer.byteBuffer());
						}
					} else {
						db.put(cfh, validWriteOptions, LLUtils.toArray(key), LLUtils.toArray(value));
					}
				}
			}
		} finally {
			if (writeOptions != null && !(writeOptions instanceof UnreleasableWriteOptions)) {
				writeOptions.close();
			}
		}
	}

	@Override
	public Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return Mono.usingWhen(rangeMono,
				rangeSend -> {
					try (var range = rangeSend.receive()) {
						if (range.isSingle()) {
							return this.containsKey(snapshot, Mono.fromCallable(range::getSingle));
						} else {
							return this.containsRange(snapshot, rangeMono);
						}
					}
				},
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		).map(isContained -> !isContained);
	}

	public Mono<Boolean> containsRange(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono) {
		return Mono.usingWhen(rangeMono,
				rangeSend -> runOnDb(() -> {
					// Temporary resources to release after finished
					Buffer cloned1 = null;
					Buffer cloned2 = null;
					Buffer cloned3 = null;
					ByteBuffer direct1 = null;
					ByteBuffer direct2 = null;
					ByteBuffer direct3 = null;
					AbstractSlice<?> slice1 = null;
					AbstractSlice<?> slice2 = null;
					try {
						try (var range = rangeSend.receive()) {
							if (Schedulers.isInNonBlockingThread()) {
								throw new UnsupportedOperationException("Called containsRange in a nonblocking thread");
							}
							try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
								readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
								readOpts.setFillCache(false);
								if (range.hasMin()) {
									try (var rangeMin = range.getMin().receive()) {
										if (databaseOptions.allowNettyDirect()) {
											var directBuf = LLUtils.convertToReadableDirect(alloc, rangeMin.send());
											cloned1 = directBuf.buffer().receive();
											direct1 = directBuf.byteBuffer();
											readOpts.setIterateLowerBound(slice1 = new DirectSlice(directBuf.byteBuffer()));
										} else {
											readOpts.setIterateLowerBound(slice1 = new Slice(LLUtils.toArray(rangeMin)));
										}
									}
								}
								if (range.hasMax()) {
									try (var rangeMax = range.getMax().receive()) {
										if (databaseOptions.allowNettyDirect()) {
											var directBuf = LLUtils.convertToReadableDirect(alloc, rangeMax.send());
											cloned2 = directBuf.buffer().receive();
											direct2 = directBuf.byteBuffer();
											readOpts.setIterateUpperBound(slice2 = new DirectSlice(directBuf.byteBuffer()));
										} else {
											readOpts.setIterateUpperBound(slice2 = new Slice(LLUtils.toArray(rangeMax)));
										}
									}
								}
								try (RocksIterator rocksIterator = db.newIterator(cfh, readOpts)) {
									if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
										try (var rangeMin = range.getMin().receive()) {
											if (databaseOptions.allowNettyDirect()) {
												var directBuf = LLUtils.convertToReadableDirect(alloc, rangeMin.send());
												cloned3 = directBuf.buffer().receive();
												direct3 = directBuf.byteBuffer();
												rocksIterator.seek(directBuf.byteBuffer());
											} else {
												rocksIterator.seek(LLUtils.toArray(rangeMin));
											}
										}
									} else {
										rocksIterator.seekToFirst();
									}
									rocksIterator.status();
									return rocksIterator.isValid();
								}
							}
						}
					} finally {
						if (cloned1 != null) cloned1.close();
						if (cloned2 != null) cloned2.close();
						if (cloned3 != null) cloned3.close();
						if (direct1 != null) PlatformDependent.freeDirectBuffer(direct1);
						if (direct2 != null) PlatformDependent.freeDirectBuffer(direct2);
						if (direct3 != null) PlatformDependent.freeDirectBuffer(direct3);
						if (slice1 != null) slice1.close();
						if (slice2 != null) slice2.close();
					}
				}).onErrorMap(cause -> new IOException("Failed to read range", cause)),
				rangeSend -> Mono.fromRunnable(rangeSend::close));
	}

	private Mono<Boolean> containsKey(@Nullable LLSnapshot snapshot, Mono<Send<Buffer>> keyMono) {
		return Mono.usingWhen(keyMono,
				keySend -> runOnDb(() -> {
					try (var key = keySend.receive()) {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called containsKey in a nonblocking thread");
						}
						StampedLock lock;
						long stamp;
						if (updateMode == UpdateMode.ALLOW) {
							lock = itemsLock.getAt(getLockIndex(key));

							stamp = lock.readLock();
						} else {
							lock = null;
							stamp = 0;
						}
						try {
							int size = RocksDB.NOT_FOUND;
							byte[] keyBytes = LLUtils.toArray(key);
							Holder<byte[]> data = new Holder<>();
							var unmodifiableReadOpts = resolveSnapshot(snapshot);
							try {
								if (db.keyMayExist(cfh, unmodifiableReadOpts, keyBytes, data)) {
									if (data.getValue() != null) {
										size = data.getValue().length;
									} else {
										size = db.get(cfh, unmodifiableReadOpts, keyBytes, NO_DATA);
									}
								}
							} finally {
								if (unmodifiableReadOpts != null && !(unmodifiableReadOpts instanceof UnreleasableReadOptions)) {
									unmodifiableReadOpts.close();
								}
							}
							return size != RocksDB.NOT_FOUND;
						} finally {
							if (updateMode == UpdateMode.ALLOW) {
								lock.unlockRead(stamp);
							}
						}
					}
				}).onErrorMap(cause -> new IOException("Failed to read", cause)),
				keySend -> Mono.fromRunnable(keySend::close)
		);
	}

	@Override
	public Mono<Send<Buffer>> put(Mono<Send<Buffer>> keyMono,
			Mono<Send<Buffer>> valueMono,
			LLDictionaryResultType resultType) {
		return Mono.usingWhen(keyMono,
				keySend -> this
						.getPreviousData(keyMono, resultType)
						.concatWith(Mono.usingWhen(valueMono,
								valueSend -> this.<Send<Buffer>>runOnDb(() -> {
									try (var key = keySend.receive()) {
										try (var value = valueSend.receive()) {
											assert key.isAccessible();
											assert value.isAccessible();
											StampedLock lock;
											long stamp;
											if (updateMode == UpdateMode.ALLOW) {
												lock = itemsLock.getAt(getLockIndex(key));

												stamp = lock.writeLock();
											} else {
												lock = null;
												stamp = 0;
											}
											try {
												if (logger.isTraceEnabled()) {
													logger.trace(MARKER_ROCKSDB, "Writing {}: {}",
															LLUtils.toStringSafe(key), LLUtils.toStringSafe(value));
												}
												dbPut(cfh, null, key.send(), value.send());
												return null;
											} finally {
												if (updateMode == UpdateMode.ALLOW) {
													lock.unlockWrite(stamp);
												}
											}
										}
									}
								}),
								value -> Mono.fromRunnable(value::close)
						).onErrorMap(cause -> new IOException("Failed to write", cause)))
						.singleOrEmpty(),
				keySend -> Mono.fromRunnable(keySend::close)
		);
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return Mono.fromSupplier(() -> updateMode);
	}

	// Remember to change also updateAndGetDelta() if you are modifying this function
	@SuppressWarnings("DuplicatedCode")
	@Override
	public Mono<Send<Buffer>> update(Mono<Send<Buffer>> keyMono,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Send<Buffer>> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono,
				keySend -> runOnDb(() -> {
					try (var key = keySend.receive()) {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called update in a nonblocking thread");
						}
						if (updateMode == UpdateMode.DISALLOW) {
							throw new UnsupportedOperationException("update() is disallowed");
						}
						StampedLock lock;
						long stamp;
						if (updateMode == UpdateMode.ALLOW) {
							lock = itemsLock.getAt(getLockIndex(key));

							stamp = lock.readLock();
						} else {
							lock = null;
							stamp = 0;
						}
						try {
							while (true) {
								@Nullable Buffer prevData;
								var prevDataHolder = existsAlmostCertainly ? null : new Holder<byte[]>();
								if (existsAlmostCertainly
										|| db.keyMayExist(cfh, LLUtils.toArray(key), prevDataHolder)) {
									if (!existsAlmostCertainly && prevDataHolder.getValue() != null) {
										byte @Nullable [] prevDataBytes = prevDataHolder.getValue();
										if (prevDataBytes != null) {
											prevData = LLUtils.fromByteArray(alloc, prevDataBytes);
										} else {
											prevData = null;
										}
									} else {
										var obtainedPrevData = dbGet(cfh, null, key.copy().send(), existsAlmostCertainly);
										if (obtainedPrevData == null) {
											prevData = null;
										} else {
											prevData = obtainedPrevData.receive();
										}
									}
								} else {
									prevData = null;
								}
								if (logger.isTraceEnabled()) {
									logger.trace(MARKER_ROCKSDB,
											"Reading {}: {} (before update)",
											LLUtils.toStringSafe(key),
											LLUtils.toStringSafe(prevData)
									);
								}
								try {
									@Nullable Buffer newData;
									try (Buffer prevDataToSendToUpdater = prevData == null ? null : prevData.copy()) {
										try (var sentData = prevDataToSendToUpdater == null ? null
												: prevDataToSendToUpdater.send()) {
											try (var newDataToReceive = updater.apply(sentData)) {
												if (newDataToReceive != null) {
													newData = newDataToReceive.receive();
												} else {
													newData = null;
												}
											}
										}
									}
									assert newData == null || newData.isAccessible();
									try {
										if (logger.isTraceEnabled()) {
											logger.trace(MARKER_ROCKSDB,
													"Updating {}. previous data: {}, updated data: {}",
													LLUtils.toStringSafe(key),
													LLUtils.toStringSafe(prevData),
													LLUtils.toStringSafe(newData)
											);
										}
										if (prevData != null && newData == null) {
											//noinspection DuplicatedCode
											if (updateMode == UpdateMode.ALLOW) {
												var ws = lock.tryConvertToWriteLock(stamp);
												if (ws != 0) {
													stamp = ws;
												} else {
													lock.unlockRead(stamp);

													stamp = lock.writeLock();
													continue;
												}
											}
											if (logger.isTraceEnabled()) {
												logger.trace(MARKER_ROCKSDB, "Deleting {} (after update)", LLUtils.toStringSafe(key));
											}
											dbDelete(cfh, null, key.send());
										} else if (newData != null
												&& (prevData == null || !LLUtils.equals(prevData, newData))) {
											//noinspection DuplicatedCode
											if (updateMode == UpdateMode.ALLOW) {
												var ws = lock.tryConvertToWriteLock(stamp);
												if (ws != 0) {
													stamp = ws;
												} else {
													lock.unlockRead(stamp);

													stamp = lock.writeLock();
													continue;
												}
											}
											if (logger.isTraceEnabled()) {
												logger.trace(MARKER_ROCKSDB,
														"Writing {}: {} (after update)",
														LLUtils.toStringSafe(key),
														LLUtils.toStringSafe(newData)
												);
											}
											Buffer dataToPut;
											if (updateReturnMode == UpdateReturnMode.GET_NEW_VALUE) {
												dataToPut = newData.copy();
											} else {
												dataToPut = newData;
											}
											try {
												dbPut(cfh, null, key.send(), dataToPut.send());
											} finally {
												if (dataToPut != newData) {
													dataToPut.close();
												}
											}
										}
										return switch (updateReturnMode) {
											case GET_NEW_VALUE -> newData != null ? newData.send() : null;
											case GET_OLD_VALUE -> prevData != null ? prevData.send() : null;
											case NOTHING -> null;
											//noinspection UnnecessaryDefault
											default -> throw new IllegalArgumentException();
										};
									} finally {
										if (newData != null) {
											newData.close();
										}
									}
								} finally {
									if (prevData != null) {
										prevData.close();
									}
								}
							}
						} finally {
							if (updateMode == UpdateMode.ALLOW) {
								lock.unlock(stamp);
							}
						}
					}
				}).onErrorMap(cause -> new IOException("Failed to read or write", cause)),
				keySend -> Mono.fromRunnable(keySend::close)
		);
	}

	// Remember to change also update() if you are modifying this function
	@SuppressWarnings("DuplicatedCode")
	@Override
	public Mono<Send<LLDelta>> updateAndGetDelta(Mono<Send<Buffer>> keyMono,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Send<Buffer>> updater,
			boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono,
				keySend -> runOnDb(() -> {
					try (var key = keySend.receive()) {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called update in a nonblocking thread");
						}
						if (updateMode == UpdateMode.DISALLOW) {
							throw new UnsupportedOperationException("update() is disallowed");
						}
						StampedLock lock;
						long stamp;
						if (updateMode == UpdateMode.ALLOW) {
							lock = itemsLock.getAt(getLockIndex(key));

							stamp = lock.readLock();
						} else {
							lock = null;
							stamp = 0;
						}
						try {
							while (true) {
								@Nullable Buffer prevData;
								var prevDataHolder = existsAlmostCertainly ? null : new Holder<byte[]>();
								if (existsAlmostCertainly
										|| db.keyMayExist(cfh, LLUtils.toArray(key), prevDataHolder)) {
									if (!existsAlmostCertainly && prevDataHolder.getValue() != null) {
										byte @Nullable [] prevDataBytes = prevDataHolder.getValue();
										if (prevDataBytes != null) {
											prevData = LLUtils.fromByteArray(alloc, prevDataBytes);
										} else {
											prevData = null;
										}
									} else {
										var obtainedPrevData = dbGet(cfh, null, key.copy().send(), existsAlmostCertainly);
										if (obtainedPrevData == null) {
											prevData = null;
										} else {
											prevData = obtainedPrevData.receive();
										}
									}
								} else {
									prevData = null;
								}
								if (logger.isTraceEnabled()) {
									logger.trace(MARKER_ROCKSDB,
											"Reading {}: {} (before update)",
											LLUtils.toStringSafe(key),
											LLUtils.toStringSafe(prevData)
									);
								}
								try {
									@Nullable Buffer newData;
									try (Buffer prevDataToSendToUpdater = prevData == null ? null : prevData.copy()) {
										try (var sentData = prevDataToSendToUpdater == null ? null
												: prevDataToSendToUpdater.send()) {
											try (var newDataToReceive = updater.apply(sentData)) {
												if (newDataToReceive != null) {
													newData = newDataToReceive.receive();
												} else {
													newData = null;
												}
											}
										}
									}
									assert newData == null || newData.isAccessible();
									try {
										if (logger.isTraceEnabled()) {
											logger.trace(MARKER_ROCKSDB,
													"Updating {}. previous data: {}, updated data: {}",
													LLUtils.toStringSafe(key),
													LLUtils.toStringSafe(prevData),
													LLUtils.toStringSafe(newData)
											);
										}
										if (prevData != null && newData == null) {
											//noinspection DuplicatedCode
											if (updateMode == UpdateMode.ALLOW) {
												var ws = lock.tryConvertToWriteLock(stamp);
												if (ws != 0) {
													stamp = ws;
												} else {
													lock.unlockRead(stamp);

													stamp = lock.writeLock();
													continue;
												}
											}
											if (logger.isTraceEnabled()) {
												logger.trace(MARKER_ROCKSDB, "Deleting {} (after update)", LLUtils.toStringSafe(key));
											}
											dbDelete(cfh, null, key.send());
										} else if (newData != null
												&& (prevData == null || !LLUtils.equals(prevData, newData))) {
											//noinspection DuplicatedCode
											if (updateMode == UpdateMode.ALLOW) {
												var ws = lock.tryConvertToWriteLock(stamp);
												if (ws != 0) {
													stamp = ws;
												} else {
													lock.unlockRead(stamp);

													stamp = lock.writeLock();
													continue;
												}
											}
											if (logger.isTraceEnabled()) {
												logger.trace(MARKER_ROCKSDB,
														"Writing {}: {} (after update)",
														LLUtils.toStringSafe(key),
														LLUtils.toStringSafe(newData)
												);
											}
											assert key.isAccessible();
											assert newData.isAccessible();
											dbPut(cfh, null, key.send(), newData.copy().send());
										}
										if (newData == prevData && newData != null) {
											newData = newData.copy();
										}
										assert (prevData == null && newData == null) || newData != prevData;
										return LLDelta.of(
												prevData != null ? prevData.send() : null,
												newData != null ? newData.send() : null
										).send();
									} finally {
										if (newData != null) {
											newData.close();
										}
									}
								} finally {
									if (prevData != null) {
										prevData.close();
									}
								}
							}
						} finally {
							if (updateMode == UpdateMode.ALLOW) {
								lock.unlock(stamp);
							}
						}
					}
				}).onErrorMap(cause -> new IOException("Failed to read or write", cause)),
				keySend -> Mono.fromRunnable(keySend::close)
		);
	}

	private void dbDelete(ColumnFamilyHandle cfh, @Nullable WriteOptions writeOptions, Send<Buffer> keyToReceive)
			throws RocksDBException {
		try (var key = keyToReceive.receive()) {
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called dbDelete in a nonblocking thread");
			}
			var validWriteOptions = Objects.requireNonNullElse(writeOptions, EMPTY_WRITE_OPTIONS);
			if (databaseOptions.allowNettyDirect()) {
				var keyNioBuffer = LLUtils.convertToReadableDirect(alloc, key.send());
				try {
					db.delete(cfh, validWriteOptions, keyNioBuffer.byteBuffer());
				} finally {
					keyNioBuffer.buffer().close();
					PlatformDependent.freeDirectBuffer(keyNioBuffer.byteBuffer());
				}
			} else {
				db.delete(cfh, validWriteOptions, LLUtils.toArray(key));
			}
		}
	}

	@Override
	public Mono<Send<Buffer>> remove(Mono<Send<Buffer>> keyMono, LLDictionaryResultType resultType) {
		return Mono.usingWhen(keyMono,
				keySend -> this
						.getPreviousData(keyMono, resultType)
						.concatWith(this
								.<Send<Buffer>>runOnDb(() -> {
									try (var key = keySend.receive()) {
										StampedLock lock;
										long stamp;
										if (updateMode == UpdateMode.ALLOW) {
											lock = itemsLock.getAt(getLockIndex(key));

											stamp = lock.writeLock();
										} else {
											lock = null;
											stamp = 0;
										}
										try {
											if (logger.isTraceEnabled()) {
												logger.trace(MARKER_ROCKSDB, "Deleting {}", LLUtils.toStringSafe(key));
											}
											dbDelete(cfh, null, key.send());
											return null;
										} finally {
											if (updateMode == UpdateMode.ALLOW) {
												lock.unlockWrite(stamp);
											}
										}
									}
								})
								.onErrorMap(cause -> new IOException("Failed to delete", cause))
						)
						.singleOrEmpty(),
				keySend -> Mono.fromRunnable(keySend::close)
		);
	}

	private Mono<Send<Buffer>> getPreviousData(Mono<Send<Buffer>> keyMono, LLDictionaryResultType resultType) {
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
									StampedLock lock;
									long stamp;
									if (updateMode == UpdateMode.ALLOW) {
										lock = itemsLock.getAt(getLockIndex(key));

										stamp = lock.readLock();
									} else {
										lock = null;
										stamp = 0;
									}
									try {
										var data = new Holder<byte[]>();
										Buffer bufferResult;
										if (db.keyMayExist(cfh, LLUtils.toArray(key), data)) {
											if (data.getValue() != null) {
												bufferResult = LLUtils.fromByteArray(alloc, data.getValue());
											} else {
												try (var bufferResultToReceive = dbGet(cfh, null, key.send(), true)) {
													bufferResult = bufferResultToReceive == null ? null : bufferResultToReceive.receive();
												}
											}
										} else {
											bufferResult = null;
										}
										try (bufferResult) {
											if (logger.isTraceEnabled()) {
												logger.trace(MARKER_ROCKSDB, "Reading {}: {}", LLUtils.toStringSafe(key), LLUtils.toStringSafe(bufferResult));
											}
											return bufferResult == null ? null : bufferResult.send();
										}
									} finally {
										if (updateMode == UpdateMode.ALLOW) {
											lock.unlockRead(stamp);
										}
									}
								}
							})
							.onErrorMap(cause -> new IOException("Failed to read ", cause)),
					keySend -> Mono.fromRunnable(keySend::close));
			case VOID -> Mono.empty();
		};
	}

	@Override
	public <K> Flux<Tuple3<K, Send<Buffer>, Optional<Send<Buffer>>>> getMulti(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<K, Send<Buffer>>> keys,
			boolean existsAlmostCertainly) {
		return keys
				.transform(normal -> new BufferTimeOutPublisher<>(normal, MULTI_GET_WINDOW, MULTI_GET_WINDOW_TIMEOUT))
				.doOnDiscard(Tuple2.class, discardedEntry -> {
					//noinspection unchecked
					var entry = (Tuple2<K, Buffer>) discardedEntry;
					entry.getT2().close();
				})
				.doOnDiscard(Tuple3.class, discardedEntry -> {
					//noinspection unchecked
					var entry = (Tuple3<K, Buffer, Buffer>) discardedEntry;
					entry.getT2().close();
					entry.getT3().close();
				})
				.flatMapSequential(keysWindow -> {
					List<Send<Buffer>> keyBufsWindowSend = new ArrayList<>(keysWindow.size());
					for (Tuple2<K, Send<Buffer>> objects : keysWindow) {
						keyBufsWindowSend.add(objects.getT2());
					}
					return runOnDb(() -> {
						List<Buffer> keyBufsWindow = new ArrayList<>(keyBufsWindowSend.size());
						for (Send<Buffer> bufferSend : keyBufsWindowSend) {
							keyBufsWindow.add(bufferSend.receive());
						}
						try {
							if (Schedulers.isInNonBlockingThread()) {
								throw new UnsupportedOperationException("Called getMulti in a nonblocking thread");
							}
							Iterable<StampedLock> locks;
							ArrayList<Long> stamps;
							if (updateMode == UpdateMode.ALLOW) {
								locks = itemsLock.bulkGetAt(getLockIndices(keyBufsWindow));
								stamps = new ArrayList<>();
								for (var lock : locks) {

									stamps.add(lock.readLock());
								}
							} else {
								locks = null;
								stamps = null;
							}
							try {
								var columnFamilyHandles = new RepeatedElementList<>(cfh, keysWindow.size());
								List<byte[]> results = db.multiGetAsList(resolveSnapshot(snapshot),
										columnFamilyHandles, LLUtils.toArray(keyBufsWindow));
								var mappedResults = new ArrayList<Tuple3<K, Send<Buffer>, Optional<Send<Buffer>>>>(results.size());
								for (int i = 0; i < results.size(); i++) {
									byte[] val = results.get(i);
									Optional<Buffer> valueOpt;
									if (val != null) {
										results.set(i, null);
										valueOpt = Optional.of(LLUtils.fromByteArray(alloc, val));
									} else {
										valueOpt = Optional.empty();
									}
									mappedResults.add(Tuples.of(keysWindow.get(i).getT1(),
											keyBufsWindow.get(i).send(),
											valueOpt.map(Resource::send)
									));
								}
								return mappedResults;
							} finally {
								if (updateMode == UpdateMode.ALLOW) {
									int index = 0;
									for (var lock : locks) {
										lock.unlockRead(stamps.get(index));
										index++;
									}
								}
							}
						} finally {
							for (Buffer buffer : keyBufsWindow) {
								buffer.close();
							}
						}
					})
					.flatMapIterable(list -> list)
					.onErrorMap(cause -> new IOException("Failed to read keys", cause))
					.doAfterTerminate(() -> keyBufsWindowSend.forEach(Send::close));
				}, 2) // Max concurrency is 2 to read data while preparing the next segment
				.doOnDiscard(LLEntry.class, ResourceSupport::close)
				.doOnDiscard(Tuple3.class, discardedEntry -> {
					if (discardedEntry.getT2() instanceof Buffer bb) {
						bb.close();
					}
					if (discardedEntry.getT2() instanceof Optional opt) {
						if (opt.isPresent() && opt.get() instanceof Buffer bb) {
							bb.close();
						}
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
								Iterable<StampedLock> locks;
								ArrayList<Long> stamps;
								if (updateMode == UpdateMode.ALLOW) {
									locks = itemsLock.bulkGetAt(getLockIndicesEntries(entriesWindow));
									stamps = new ArrayList<>();
									for (var lock : locks) {
										stamps.add(lock.writeLock());
									}
								} else {
									locks = null;
									stamps = null;
								}
								try {
									ArrayList<Send<LLEntry>> oldValues;
									if (getOldValues) {
										oldValues = new ArrayList<>(entriesWindow.size());
										try (var readOptions = resolveSnapshot(null)) {
											for (LLEntry entry : entriesWindow) {
												try (var key = entry.getKey().receive()) {
													Send<Buffer> oldValue = dbGet(cfh, readOptions, key.copy().send(), false);
													if (oldValue != null) {
														oldValues.add(LLEntry.of(key.send(), oldValue).send());
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
											if (databaseOptions.allowNettyDirect()) {
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
											var k = LLUtils.convertToReadableDirect(alloc, entry.getKey());
											try {
												var v = LLUtils.convertToReadableDirect(alloc, entry.getValue());
												try {
													db.put(cfh, EMPTY_WRITE_OPTIONS, k.byteBuffer(), v.byteBuffer());
												} finally {
													v.buffer().close();
													PlatformDependent.freeDirectBuffer(v.byteBuffer());
												}
											} finally {
												k.buffer().close();
												PlatformDependent.freeDirectBuffer(k.byteBuffer());
											}
										}
									}
									return oldValues;
								} finally {
									if (updateMode == UpdateMode.ALLOW) {
										int index = 0;
										for (var lock : locks) {
											lock.unlockWrite(stamps.get(index));
											index++;
										}
									}
								}
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
	public <X> Flux<ExtraKeyOperationResult<Send<Buffer>, X>> updateMulti(Flux<Tuple2<Send<Buffer>, X>> entries,
			BiSerializationFunction<Send<Buffer>, X, Send<Buffer>> updateFunction) {
		return entries
				.buffer(Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.flatMapSequential(ew -> this.<Iterable<ExtraKeyOperationResult<Send<Buffer>, X>>>runOnDb(() -> {
					List<Tuple2<Buffer, X>> entriesWindow = new ArrayList<>(ew.size());
					for (Tuple2<Send<Buffer>, X> tuple : ew) {
						entriesWindow.add(tuple.mapT1(Send::receive));
					}
					try {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called updateMulti in a nonblocking thread");
						}
						List<Buffer> keyBufsWindow = new ArrayList<>(entriesWindow.size());
						for (Tuple2<Buffer, X> objects : entriesWindow) {
							keyBufsWindow.add(objects.getT1());
						}

						Iterable<StampedLock> locks;
						ArrayList<Long> stamps;
						if (updateMode == UpdateMode.ALLOW) {
							locks = itemsLock.bulkGetAt(getLockIndicesWithExtra(entriesWindow));
							stamps = new ArrayList<>();
							for (var lock : locks) {
								stamps.add(lock.writeLock());
							}
						} else {
							locks = null;
							stamps = null;
						}
						try {
							var columnFamilyHandles = new RepeatedElementList<>(cfh, entriesWindow.size());
							ArrayList<Tuple3<Send<Buffer>, X, Optional<Send<Buffer>>>> mappedInputs;
							{
								var inputs = db.multiGetAsList(resolveSnapshot(null), columnFamilyHandles,
										LLUtils.toArray(keyBufsWindow));
								mappedInputs = new ArrayList<>(inputs.size());
								for (int i = 0; i < inputs.size(); i++) {
									var val = inputs.get(i);
									if (val != null) {
										inputs.set(i, null);
										mappedInputs.add(Tuples.of(
												keyBufsWindow.get(i).send(),
												entriesWindow.get(i).getT2(),
												Optional.of(fromByteArray(alloc, val).send())
										));
									} else {
										mappedInputs.add(Tuples.of(
												keyBufsWindow.get(i).send(),
												entriesWindow.get(i).getT2(),
												Optional.empty()
										));
									}
								}
							}
							var updatedValuesToWrite = new ArrayList<Send<Buffer>>(mappedInputs.size());
							var valueChangedResult = new ArrayList<ExtraKeyOperationResult<Send<Buffer>, X>>(mappedInputs.size());
							try {
								for (var mappedInput : mappedInputs) {
									try (var updatedValue = updateFunction
											.apply(mappedInput.getT1(), mappedInput.getT2()).receive()) {
										try (var t3 = mappedInput.getT3().map(Send::receive).orElse(null)) {
											valueChangedResult.add(new ExtraKeyOperationResult<>(mappedInput.getT1(),
													mappedInput.getT2(), !LLUtils.equals(t3, updatedValue)));
										}
										updatedValuesToWrite.add(updatedValue.send());
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
								for (Tuple2<Buffer, X> entry : entriesWindow) {
									var valueToWrite = updatedValuesToWrite.get(i);
									if (valueToWrite == null) {
										batch.delete(cfh, entry.getT1().send());
									} else {
										batch.put(cfh, entry.getT1().send(), valueToWrite);
									}
									i++;
								}
								batch.writeToDbAndClose();
								batch.close();
							} else {
								int i = 0;
								for (Tuple2<Buffer, X> entry : entriesWindow) {
									var k = LLUtils.convertToReadableDirect(alloc, entry.getT1().send());
									try {
										var v = LLUtils.convertToReadableDirect(alloc, updatedValuesToWrite.get(i));
										try {
											db.put(cfh, EMPTY_WRITE_OPTIONS, k.byteBuffer(), v.byteBuffer());
										} finally {
											v.buffer().close();
											PlatformDependent.freeDirectBuffer(v.byteBuffer());
										}
									} finally {
										k.buffer().close();
										PlatformDependent.freeDirectBuffer(k.byteBuffer());
									}
									i++;
								}
							}
							return valueChangedResult;
						} finally {
							if (updateMode == UpdateMode.ALLOW) {
								int index = 0;
								for (var lock : locks) {
									lock.unlockWrite(stamps.get(index));
									index++;
								}
							}
						}
					} finally {
						for (Tuple2<Buffer, X> tuple : entriesWindow) {
							tuple.getT1().close();
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
				})
				.doOnDiscard(ExtraKeyOperationResult.class, entry -> {
					if (entry.key() instanceof Buffer bb) {
						bb.close();
					}
					if (entry.extra() instanceof Buffer bb) {
						bb.close();
					}
				})
				.doOnDiscard(Collection.class, obj -> {
					//noinspection unchecked
					var castedEntries = (Collection<ExtraKeyOperationResult<Object, Object>>) obj;
					for (var entry : castedEntries) {
						if (entry.key() instanceof Buffer bb) {
							bb.close();
						}
						if (entry.extra() instanceof Buffer bb) {
							bb.close();
						}
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
						() -> new LLLocalEntryReactiveRocksIterator(db, alloc, cfh, rangeSend,
								databaseOptions.allowNettyDirect(), resolveSnapshot(snapshot), getRangeMultiDebugName),
						llLocalEntryReactiveRocksIterator -> llLocalEntryReactiveRocksIterator.flux().subscribeOn(dbScheduler),
						LLLocalReactiveRocksIterator::release
				).transform(LLUtils::handleDiscard),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	private Flux<List<Send<LLEntry>>> getRangeMultiGrouped(LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono, int prefixLength) {
		return Flux.usingWhen(rangeMono,
				rangeSend -> Flux.using(
						() -> new LLLocalGroupedEntryReactiveRocksIterator(db, alloc, cfh, prefixLength, rangeSend,
								databaseOptions.allowNettyDirect(), resolveSnapshot(snapshot), "getRangeMultiGrouped"),
						reactiveRocksIterator -> reactiveRocksIterator.flux().subscribeOn(dbScheduler),
						LLLocalGroupedReactiveRocksIterator::release
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
						() -> new LLLocalGroupedKeyReactiveRocksIterator(db, alloc, cfh, prefixLength, rangeSend,
								databaseOptions.allowNettyDirect(), resolveSnapshot(snapshot), "getRangeKeysGrouped"),
						reactiveRocksIterator -> reactiveRocksIterator.flux().subscribeOn(dbScheduler),
						LLLocalGroupedReactiveRocksIterator::release
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
										databaseOptions.allowNettyDirect(), ro, range.send(), db, cfh);
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
	public Flux<Send<Buffer>> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, Mono<Send<LLRange>> rangeMono, int prefixLength) {
		return Flux.usingWhen(rangeMono,
				rangeSend -> Flux
						.using(
								() -> new LLLocalKeyPrefixReactiveRocksIterator(db,
										alloc,
										cfh,
										prefixLength,
										rangeSend,
										databaseOptions.allowNettyDirect(),
										resolveSnapshot(snapshot),
										true,
										"getRangeKeysGrouped"
								),
								LLLocalKeyPrefixReactiveRocksIterator::flux,
								LLLocalKeyPrefixReactiveRocksIterator::release
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
						() -> new LLLocalKeyReactiveRocksIterator(db, alloc, cfh, rangeSend,
								databaseOptions.allowNettyDirect(), resolveSnapshot(snapshot)
						),
						llLocalKeyReactiveRocksIterator -> llLocalKeyReactiveRocksIterator.flux().subscribeOn(dbScheduler),
						LLLocalReactiveRocksIterator::release
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
													minBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(),
															opts,
															IterateBound.LOWER,
															range.getMin()
													);
												} else {
													minBound = emptyReleasableSlice();
												}
												try {
													ReleasableSlice maxBound;
													if (range.hasMax()) {
														maxBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(),
																opts,
																IterateBound.UPPER,
																range.getMax()
														);
													} else {
														maxBound = emptyReleasableSlice();
													}
													assert cfh.isOwningHandle();
													assert opts.isOwningHandle();
													SafeCloseable seekTo;
													try (RocksIterator it = db.newIterator(cfh, opts)) {
														if (!PREFER_SEEK_TO_FIRST && range.hasMin()) {
															seekTo = rocksIterSeekTo(alloc, databaseOptions.allowNettyDirect(), it, range.getMin());
														} else {
															seekTo = null;
															it.seekToFirst();
														}
														try {
															it.status();
															while (it.isValid()) {
																db.delete(cfh, it.key());
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
																var k = LLUtils.convertToReadableDirect(alloc, entry.getKey());
																try {
																	var v = LLUtils.convertToReadableDirect(alloc, entry.getValue());
																	try {
																		db.put(cfh, EMPTY_WRITE_OPTIONS, k.byteBuffer(), v.byteBuffer());
																	} finally {
																		v.buffer().close();
																		PlatformDependent.freeDirectBuffer(v.byteBuffer());
																	}
																} finally {
																	k.buffer().close();
																	PlatformDependent.freeDirectBuffer(k.byteBuffer());
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
																for (LLEntry entry : entriesList) {
																	assert entry.isAccessible();
																	if (databaseOptions.allowNettyDirect()) {
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
								throw new UnsupportedOperationException("Can't use write batches in setRange without window. Please fix params");
							});
						}
						return this
								.getRange(null, rangeMono, false)
								.flatMap(oldValueSend -> this.<Void>runOnDb(() -> {
									try (var oldValue = oldValueSend.receive()) {
										dbDelete(cfh, EMPTY_WRITE_OPTIONS, oldValue.getKey());
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
				minBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.LOWER, range.getMin());
			} else {
				minBound = emptyReleasableSlice();
			}
			try {
				ReleasableSlice maxBound;
				if (range.hasMax()) {
					maxBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.UPPER, range.getMax());
				} else {
					maxBound = emptyReleasableSlice();
				}
				try (var rocksIterator = db.newIterator(cfh, readOpts)) {
					SafeCloseable seekTo;
					if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
						seekTo = rocksIterSeekTo(alloc, databaseOptions.allowNettyDirect(), rocksIterator, range.getMin());
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
					minBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.LOWER, range.getMin());
				} else {
					minBound = emptyReleasableSlice();
				}
				try {
					ReleasableSlice maxBound;
					if (range.hasMax()) {
						maxBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.UPPER,
								range.getMax());
					} else {
						maxBound = emptyReleasableSlice();
					}
					try (var rocksIterator = db.newIterator(cfh, readOpts)) {
						SafeCloseable seekTo;
						if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
							seekTo = rocksIterSeekTo(alloc, databaseOptions.allowNettyDirect(), rocksIterator, range.getMin());
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
	private static SafeCloseable rocksIterSeekTo(BufferAllocator alloc, boolean allowNettyDirect, RocksIterator rocksIterator,
			Send<Buffer> bufferToReceive) {
		try (var buffer = bufferToReceive.receive()) {
			if (allowNettyDirect) {
				var direct = LLUtils.convertToReadableDirect(alloc, buffer.send());
				assert direct.byteBuffer().isDirect();
				rocksIterator.seek(direct.byteBuffer());
				return () -> {
					direct.buffer().close();
					PlatformDependent.freeDirectBuffer(direct.byteBuffer());
				};
			} else {
				rocksIterator.seek(LLUtils.toArray(buffer));
				return null;
			}
		}
	}

	private static ReleasableSlice setIterateBound(BufferAllocator alloc, boolean allowNettyDirect, ReadOptions readOpts,
			IterateBound boundType, Send<Buffer> bufferToReceive) {
		var buffer = bufferToReceive.receive();
		try {
			requireNonNull(buffer);
			AbstractSlice<?> slice;
			if (allowNettyDirect && LLLocalDictionary.USE_DIRECT_BUFFER_BOUNDS) {
				var direct = LLUtils.convertToReadableDirect(alloc, buffer.send());
				buffer = direct.buffer().receive();
				assert direct.byteBuffer().isDirect();
				slice = new DirectSlice(direct.byteBuffer(), buffer.readableBytes());
				assert slice.size() == buffer.readableBytes();
				assert slice.compare(new Slice(LLUtils.toArray(buffer))) == 0;
				if (boundType == IterateBound.LOWER) {
					readOpts.setIterateLowerBound(slice);
				} else {
					readOpts.setIterateUpperBound(slice);
				}
				return new ReleasableSliceImpl(slice, buffer, direct.byteBuffer());
			} else {
				try {
					slice = new Slice(requireNonNull(LLUtils.toArray(buffer)));
					if (boundType == IterateBound.LOWER) {
						readOpts.setIterateLowerBound(slice);
					} else {
						readOpts.setIterateUpperBound(slice);
					}
					return new ReleasableSliceImpl(slice, null, null);
				} finally {
					buffer.close();
				}
			}
		} catch (Throwable e) {
			buffer.close();
			throw e;
		}
	}

	private static ReleasableSlice emptyReleasableSlice() {
		var arr = new byte[0];

		return new SimpleSliceWithoutRelease(new Slice(arr), null, arr);
	}

	public static record SimpleSliceWithoutRelease(AbstractSlice<?> slice, @Nullable Buffer byteBuf,
																								 @Nullable Object additionalData) implements ReleasableSlice {}

	public static record ReleasableSliceImpl(AbstractSlice<?> slice, @Nullable Buffer byteBuf,
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
							try (RocksIterator rocksIterator = db.newIterator(cfh, readOpts)) {
								rocksIterator.seekToLast();

								rocksIterator.status();
								if (rocksIterator.isValid()) {
									firstDeletedKey = FIRST_KEY;
									lastDeletedKey = rocksIterator.key();
									writeBatch.deleteRange(cfh, FIRST_KEY, rocksIterator.key());
									writeBatch.delete(cfh, rocksIterator.key());
								}
							}

							writeBatch.writeToDbAndClose();


							// Compact range
							db.suggestCompactRange(cfh);
							if (firstDeletedKey != null && lastDeletedKey != null) {
								db.compactRange(cfh,
										firstDeletedKey,
										lastDeletedKey,
										new CompactRangeOptions()
												.setAllowWriteStall(false)
												.setExclusiveManualCompaction(false)
												.setChangeLevel(false)
								);
							}

							db.flush(new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true), cfh);
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
		return Mono.usingWhen(rangeMono,
				rangeSend -> {
					try (var range = rangeSend.receive()) {
						if (Schedulers.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called sizeRange in a nonblocking thread");
						}
						if (range.isAll()) {
							return this
									.runOnDb(() -> fast ? fastSizeAll(snapshot) : exactSizeAll(snapshot))
									.onErrorMap(IOException::new);
						} else {
							return runOnDb(() -> {
								try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
									readOpts.setFillCache(false);
									readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
									ReleasableSlice minBound;
									if (range.hasMin()) {
										minBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.LOWER,
												range.getMin());
									} else {
										minBound = emptyReleasableSlice();
									}
									try {
										ReleasableSlice maxBound;
										if (range.hasMax()) {
											maxBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.UPPER,
													range.getMax());
										} else {
											maxBound = emptyReleasableSlice();
										}
										try {
											if (fast) {
												readOpts.setIgnoreRangeDeletions(true);

											}
											try (var rocksIterator = db.newIterator(cfh, readOpts)) {
												SafeCloseable seekTo;
												if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
													seekTo = rocksIterSeekTo(alloc, databaseOptions.allowNettyDirect(), rocksIterator,
															range.getMin());
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
							}).onErrorMap(cause -> new IOException("Failed to get size of range " + range, cause));
						}
					}
				},
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
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
								minBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.LOWER,
										range.getMin());
							} else {
								minBound = emptyReleasableSlice();
							}
							try {
								ReleasableSlice maxBound;
								if (range.hasMax()) {
									maxBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.UPPER,
											range.getMax());
								} else {
									maxBound = emptyReleasableSlice();
								}
								try (var rocksIterator = db.newIterator(cfh, readOpts)) {
									SafeCloseable seekTo;
									if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
										seekTo = rocksIterSeekTo(alloc, databaseOptions.allowNettyDirect(), rocksIterator, range.getMin());
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
								minBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.LOWER,
										range.getMin());
							} else {
								minBound = emptyReleasableSlice();
							}
							try {
								ReleasableSlice maxBound;
								if (range.hasMax()) {
									maxBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.UPPER,
											range.getMax());
								} else {
									maxBound = emptyReleasableSlice();
								}
								try (var rocksIterator = db.newIterator(cfh, readOpts)) {
									SafeCloseable seekTo;
									if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
										seekTo = rocksIterSeekTo(alloc, databaseOptions.allowNettyDirect(), rocksIterator, range.getMin());
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
					return db.getLongProperty(cfh, "rocksdb.estimate-num-keys");
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
				try (RocksIterator rocksIterator = db.newIterator(cfh, rocksdbSnapshot)) {
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
									try (RocksIterator rocksIterator = db.newIterator(cfh, rangeReadOpts)) {
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
						.collect(Collectors.toList());
				long count = 0;
				for (ForkJoinTask<Long> future : futures) {
					count += future.join();
				}
				return count;
			} else {
				long count = 0;
				try (RocksIterator iter = db.newIterator(cfh, readOpts)) {
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
								minBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.LOWER,
										range.getMin());
							} else {
								minBound = emptyReleasableSlice();
							}
							try {
								ReleasableSlice maxBound;
								if (range.hasMax()) {
									maxBound = setIterateBound(alloc, databaseOptions.allowNettyDirect(), readOpts, IterateBound.UPPER,
											range.getMax());
								} else {
									maxBound = emptyReleasableSlice();
								}
								try (RocksIterator rocksIterator = db.newIterator(cfh, readOpts)) {
									SafeCloseable seekTo;
									if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
										seekTo = rocksIterSeekTo(alloc, databaseOptions.allowNettyDirect(), rocksIterator, range.getMin());
									} else {
										seekTo = null;
										rocksIterator.seekToFirst();
									}
									try {
										rocksIterator.status();
										if (!rocksIterator.isValid()) {
											return null;
										}
										try (Buffer key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key)) {
											try (Buffer value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value)) {
												dbDelete(cfh, null, key.copy().send());
												return LLEntry.of(key.send(), value.send()).send();
											}
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
				}).onErrorMap(cause -> new IOException("Failed to delete", cause)),
				rangeSend -> Mono.fromRunnable(rangeSend::close)
		);
	}

	@NotNull
	public static Tuple4<RocksIterator, ReleasableSlice, ReleasableSlice, SafeCloseable> getRocksIterator(BufferAllocator alloc,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			Send<LLRange> rangeToReceive,
			RocksDB db,
			ColumnFamilyHandle cfh) {
		try (var range = rangeToReceive.receive()) {
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called getRocksIterator in a nonblocking thread");
			}
			ReleasableSlice sliceMin;
			ReleasableSlice sliceMax;
			if (range.hasMin()) {
				sliceMin = setIterateBound(alloc, allowNettyDirect, readOptions, IterateBound.LOWER, range.getMin());
			} else {
				sliceMin = emptyReleasableSlice();
			}
			if (range.hasMax()) {
				sliceMax = setIterateBound(alloc, allowNettyDirect, readOptions, IterateBound.UPPER, range.getMax());
			} else {
				sliceMax = emptyReleasableSlice();
			}
			var rocksIterator = db.newIterator(cfh, readOptions);
			SafeCloseable seekTo;
			if (!PREFER_SEEK_TO_FIRST && range.hasMin()) {
				seekTo = Objects.requireNonNullElseGet(rocksIterSeekTo(alloc, allowNettyDirect, rocksIterator, range.getMin()),
						() -> ((SafeCloseable) () -> {})
				);
			} else {
				seekTo = () -> {};
				rocksIterator.seekToFirst();
			}
			return Tuples.of(rocksIterator, sliceMin, sliceMax, seekTo);
		}
	}
}
