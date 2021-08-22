package it.cavallium.dbengine.database.disk;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.ExtraKeyOperationResult;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.RepeatedElementList;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.BiSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;
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
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

@NotAtomic
public class LLLocalDictionary implements LLDictionary {

	protected static final Logger logger = LoggerFactory.getLogger(LLLocalDictionary.class);
	private static final boolean USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS = false;
	static final int RESERVED_WRITE_BATCH_SIZE = 2 * 1024 * 1024; // 2MiB
	static final long MAX_WRITE_BATCH_SIZE = 1024L * 1024L * 1024L; // 1GiB
	static final int CAPPED_WRITE_BATCH_CAP = 50000; // 50K operations
	static final int MULTI_GET_WINDOW = 500;
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
	public static final boolean DEBUG_PREFIXES_WHEN_ASSERTIONS_ARE_ENABLED = true;
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
	private final ByteBufAllocator alloc;
	private final String getRangeMultiDebugName;
	private final String getRangeKeysMultiDebugName;
	private final DatabaseOptions databaseOptions;

	public LLLocalDictionary(
			ByteBufAllocator allocator,
			@NotNull RocksDB db,
			@NotNull ColumnFamilyHandle columnFamilyHandle,
			String databaseName,
			String columnName,
			Scheduler dbScheduler,
			Function<LLSnapshot, Snapshot> snapshotResolver,
			UpdateMode updateMode,
			DatabaseOptions databaseOptions) {
		Objects.requireNonNull(db);
		this.db = db;
		Objects.requireNonNull(columnFamilyHandle);
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

	private int getLockIndex(ByteBuf key) {
		return Math.abs(LLUtils.hashCode(key) % STRIPES);
	}

	private IntArrayList getLockIndices(List<ByteBuf> keys) {
		var list = new IntArrayList(keys.size());
		for (ByteBuf key : keys) {
			list.add(getLockIndex(key));
		}
		return list;
	}

	private IntArrayList getLockIndicesEntries(List<Entry<ByteBuf, ByteBuf>> keys) {
		var list = new IntArrayList(keys.size());
		for (Entry<ByteBuf, ByteBuf> key : keys) {
			list.add(getLockIndex(key.getKey()));
		}
		return list;
	}

	private <X> IntArrayList getLockIndicesWithExtra(List<Tuple2<ByteBuf, X>> entries) {
		var list = new IntArrayList(entries.size());
		for (Tuple2<ByteBuf, X> key : entries) {
			list.add(getLockIndex(key.getT1()));
		}
		return list;
	}

	@Override
	public ByteBufAllocator getAllocator() {
		return alloc;
	}

	private <T> Mono<T> runOnDb(Callable<@Nullable T> callable) {
		return Mono.fromCallable(callable).subscribeOn(dbScheduler);
	}

	@Override
	public Mono<ByteBuf> get(@Nullable LLSnapshot snapshot,
			Mono<ByteBuf> keyMono,
			boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono,
				key -> runOnDb(() -> {
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
						if (logger.isTraceEnabled()) {
							logger.trace("Reading {}", LLUtils.toStringSafe(key));
						}
						return dbGet(cfh, resolveSnapshot(snapshot), key.retain(), existsAlmostCertainly);
					} finally {
						if (updateMode == UpdateMode.ALLOW) {
							lock.unlockRead(stamp);
						}
					}
				}).onErrorMap(cause -> new IOException("Failed to read "
						+ LLUtils.toStringSafe(key), cause)),
				key -> Mono.fromRunnable(key::release)
		);
	}

	private ByteBuf dbGet(ColumnFamilyHandle cfh,
			@Nullable ReadOptions readOptions,
			ByteBuf key,
			boolean existsAlmostCertainly) throws RocksDBException {
		try {
			if (databaseOptions.allowNettyDirect() && key.isDirect()) {

				//todo: implement keyMayExist if existsAlmostCertainly is false.
				// Unfortunately it's not feasible until RocksDB implements keyMayExist with buffers

				// Create the key nio buffer to pass to RocksDB
				if (!key.isDirect()) {
					throw new RocksDBException("Key buffer must be direct");
				}
				ByteBuffer keyNioBuffer = LLUtils.toDirect(key);
				assert !databaseOptions.enableDbAssertionsWhenUsingAssertions() || keyNioBuffer.isDirect();
				// Create a direct result buffer because RocksDB works only with direct buffers
				ByteBuf resultBuf = alloc.directBuffer(INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES);
				try {
					int valueSize;
					int assertionReadData = -1;
					ByteBuffer resultNioBuf;
					do {
						// Create the result nio buffer to pass to RocksDB
						resultNioBuf = resultBuf.nioBuffer(0, resultBuf.capacity());
						if (databaseOptions.enableDbAssertionsWhenUsingAssertions()) {
							assert keyNioBuffer.isDirect();
							assert resultNioBuf.isDirect();
						}
						valueSize = db.get(cfh,
								Objects.requireNonNullElse(readOptions, EMPTY_READ_OPTIONS),
								keyNioBuffer.position(0),
								resultNioBuf
						);
						if (valueSize != RocksDB.NOT_FOUND) {
							if (databaseOptions.enableDbAssertionsWhenUsingAssertions()) {
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
								return resultBuf.setIndex(0, valueSize).retain();
							} else {
								// If the locking is enabled the data is safe, so we can append the next read data.
								// Otherwise we need to re-read everything.
								if (updateMode == UpdateMode.ALLOW) {
									// Update the resultBuf writerIndex with the new position
									resultBuf.writerIndex(resultNioBuf.limit());
								}
								//noinspection UnusedAssignment
								resultNioBuf = null;
							}
							// Rewind the keyNioBuf position, making it readable again for the next loop iteration
							keyNioBuffer.rewind();
							if (resultBuf.capacity() < valueSize) {
								// Expand the resultBuf size if the result is bigger than the current result
								// buffer size
								resultBuf.capacity(valueSize);
							}
						}
						// Repeat if the result has been found but it's still not finished
					} while (valueSize != RocksDB.NOT_FOUND);
					// If the value is not found return null
					return null;
				} finally {
					resultBuf.release();
				}
			} else {
				ReadOptions validReadOptions = Objects.requireNonNullElse(readOptions, EMPTY_READ_OPTIONS);
				byte[] keyArray = LLUtils.toArray(key);
				Objects.requireNonNull(keyArray);
				Holder<byte[]> data = existsAlmostCertainly ? null : new Holder<>();
				if (existsAlmostCertainly || db.keyMayExist(cfh,
						validReadOptions,
						keyArray,
						data
				)) {
					if (!existsAlmostCertainly && data.getValue() != null) {
						return wrappedBuffer(data.getValue());
					} else {
						byte[] result = db.get(cfh, validReadOptions, keyArray);
						if (result == null) {
							return null;
						} else {
							return wrappedBuffer(result);
						}
					}
				} else {
					return null;
				}
			}
		} finally {
			key.release();
		}
	}

	@SuppressWarnings("SameParameterValue")
	private void dbPut(ColumnFamilyHandle cfh,
			@Nullable WriteOptions writeOptions,
			ByteBuf key,
			ByteBuf value) throws RocksDBException {
		try {
			WriteOptions validWriteOptions = Objects.requireNonNullElse(writeOptions, EMPTY_WRITE_OPTIONS);
			if (databaseOptions.allowNettyDirect() && key.isDirect() && value.isDirect()) {
				if (!key.isDirect()) {
					throw new RocksDBException("Key buffer must be direct");
				}
				if (!value.isDirect()) {
					throw new RocksDBException("Value buffer must be direct");
				}
				var keyNioBuffer = LLUtils.toDirect(key);
				assert !databaseOptions.enableDbAssertionsWhenUsingAssertions() || keyNioBuffer.isDirect();


				var valueNioBuffer = LLUtils.toDirect(value);
				assert !databaseOptions.enableDbAssertionsWhenUsingAssertions() || valueNioBuffer.isDirect();
				db.put(cfh, validWriteOptions, keyNioBuffer, valueNioBuffer);
			} else {
				db.put(cfh, validWriteOptions, LLUtils.toArray(key), LLUtils.toArray(value));
			}
		} finally {
			key.release();
			value.release();
		}
	}

	@Override
	public Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return Mono.usingWhen(rangeMono,
				range -> {
					if (range.isSingle()) {
						return this.containsKey(snapshot, Mono.just(range.getSingle()).map(ByteBuf::retain));
					} else {
						return this.containsRange(snapshot, Mono.just(range).map(LLRange::retain));
					}
				},
				range -> Mono.fromRunnable(range::release)
		).map(isContained -> !isContained);
	}

	public Mono<Boolean> containsRange(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return Mono.usingWhen(rangeMono,
				range -> runOnDb(() -> {
					try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
						readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
						readOpts.setFillCache(false);
						if (range.hasMin()) {
							if (databaseOptions.allowNettyDirect() && range.getMin().isDirect()) {
								readOpts.setIterateLowerBound(new DirectSlice(Objects
										.requireNonNull(LLUtils.toDirect(range.getMin()),
												"This range must use direct buffers")));
							} else {
								readOpts.setIterateLowerBound(new Slice(LLUtils.toArray(range.getMin())));
							}
						}
						if (range.hasMax()) {
							if (databaseOptions.allowNettyDirect() && range.getMax().isDirect()) {
								readOpts.setIterateUpperBound(new DirectSlice(Objects
										.requireNonNull(LLUtils.toDirect(range.getMax()),
										"This range must use direct buffers"
								)));
							} else {
								readOpts.setIterateUpperBound(new Slice(LLUtils.toArray(range.getMax())));
							}
						}
						try (RocksIterator rocksIterator = db.newIterator(cfh, readOpts)) {
							if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
								if (databaseOptions.allowNettyDirect() && range.getMin().isDirect()) {
									rocksIterator.seek(Objects.requireNonNull(LLUtils.toDirect(range.getMin()),
											"This range must use direct buffers"
									));
								} else {
									rocksIterator.seek(LLUtils.toArray(range.getMin()));
								}
							} else {
								rocksIterator.seekToFirst();
							}
							rocksIterator.status();
							return rocksIterator.isValid();
						}
					}
				}).onErrorMap(cause -> new IOException("Failed to read range " + range.toString(), cause)),
				range -> Mono.fromRunnable(range::release));
	}

	private Mono<Boolean> containsKey(@Nullable LLSnapshot snapshot, Mono<ByteBuf> keyMono) {
		return Mono.usingWhen(keyMono,
				key -> runOnDb(() -> {

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
						if (db.keyMayExist(cfh, unmodifiableReadOpts, keyBytes, data)) {
							if (data.getValue() != null) {
								size = data.getValue().length;
							} else {
								size = db.get(cfh, unmodifiableReadOpts, keyBytes, NO_DATA);
							}
						}
						return size != RocksDB.NOT_FOUND;
					} finally {
						if (updateMode == UpdateMode.ALLOW) {
							lock.unlockRead(stamp);
						}
					}
				}).onErrorMap(cause -> new IOException("Failed to read "
						+ LLUtils.toStringSafe(key), cause)),
				key -> Mono.fromRunnable(key::release)
		);
	}

	@Override
	public Mono<ByteBuf> put(Mono<ByteBuf> keyMono,
			Mono<ByteBuf> valueMono,
			LLDictionaryResultType resultType) {
		return Mono.usingWhen(keyMono,
				key -> this
						.getPreviousData(Mono.just(key).map(ByteBuf::retain), resultType)
						.concatWith(Mono.usingWhen(valueMono,
								value -> this.<ByteBuf>runOnDb(() -> {
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
											logger.trace("Writing {}: {}",
													LLUtils.toStringSafe(key), LLUtils.toStringSafe(value));
										}
										dbPut(cfh, null, key.retain(), value.retain());
										return null;
									} finally {
										if (updateMode == UpdateMode.ALLOW) {
											lock.unlockWrite(stamp);
										}
									}
								}),
								value -> Mono.fromRunnable(value::release)
						).onErrorMap(cause -> new IOException("Failed to write "
								+ LLUtils.toStringSafe(key), cause)))
						.singleOrEmpty(),
				key -> Mono.fromRunnable(key::release)
		);
	}

	@Override
	public Mono<UpdateMode> getUpdateMode() {
		return Mono.fromSupplier(() -> updateMode);
	}

	// Remember to change also updateAndGetDelta() if you are modifying this function
	@SuppressWarnings("DuplicatedCode")
	@Override
	public Mono<ByteBuf> update(Mono<ByteBuf> keyMono,
			SerializationFunction<@Nullable ByteBuf, @Nullable ByteBuf> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono,
				key -> runOnDb(() -> {
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
						if (logger.isTraceEnabled()) {
							logger.trace("Reading {}", LLUtils.toStringSafe(key));
						}
						while (true) {
							@Nullable ByteBuf prevData;
							var prevDataHolder = existsAlmostCertainly ? null : new Holder<byte[]>();
							if (existsAlmostCertainly
									|| db.keyMayExist(cfh, LLUtils.toArray(key), prevDataHolder)) {
								if (!existsAlmostCertainly && prevDataHolder.getValue() != null) {
									byte @Nullable [] prevDataBytes = prevDataHolder.getValue();
									if (prevDataBytes != null) {
										prevData = wrappedBuffer(prevDataBytes);
									} else {
										prevData = null;
									}
								} else {
									prevData = dbGet(cfh, null, key.retain(), existsAlmostCertainly);
								}
							} else {
								prevData = null;
							}
							try {
								@Nullable ByteBuf newData;
								ByteBuf prevDataToSendToUpdater = prevData == null
										? null
										: prevData.retainedSlice();
								try {
									newData = updater.apply(prevDataToSendToUpdater == null
											? null
											: prevDataToSendToUpdater.retain());
									if (!(prevDataToSendToUpdater == null
											|| prevDataToSendToUpdater.readerIndex() == 0
											|| !prevDataToSendToUpdater.isReadable())) {
										throw new IllegalStateException("The updater has read the previous data partially"
												+ " (read bytes: " + prevDataToSendToUpdater.readerIndex()
												+ " unread bytes: " + prevDataToSendToUpdater.readableBytes() + ")."
												+ " The only allowed options are reading the data fully or not reading it at all");
									}
								} finally {
									if (prevDataToSendToUpdater != null) {
										prevDataToSendToUpdater.release();
									}
								}
								try {
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
											logger.trace("Deleting {}", LLUtils.toStringSafe(key));
										}
										dbDelete(cfh, null, key.retain());
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
											logger.trace("Writing {}: {}",
													LLUtils.toStringSafe(key), LLUtils.toStringSafe(newData));
										}
										dbPut(cfh, null, key.retain(), newData.retain());
									}
									return switch (updateReturnMode) {
										case GET_NEW_VALUE -> newData != null ? newData.retain() : null;
										case GET_OLD_VALUE -> prevData != null ? prevData.retain() : null;
										case NOTHING -> null;
										//noinspection UnnecessaryDefault
										default -> throw new IllegalArgumentException();
									};
								} finally {
									if (newData != null) {
										newData.release();
									}
								}
							} finally {
								if (prevData != null) {
									prevData.release();
								}
							}
						}
					} finally {
						if (updateMode == UpdateMode.ALLOW) {
							lock.unlock(stamp);
						}
					}
				}).onErrorMap(cause -> new IOException("Failed to read or write "
						+ LLUtils.toStringSafe(key), cause)),
				key -> Mono.fromRunnable(key::release)
		);
	}

	// Remember to change also update() if you are modifying this function
	@SuppressWarnings("DuplicatedCode")
	@Override
	public Mono<Delta<ByteBuf>> updateAndGetDelta(Mono<ByteBuf> keyMono,
			SerializationFunction<@Nullable ByteBuf, @Nullable ByteBuf> updater,
			boolean existsAlmostCertainly) {
		return Mono.usingWhen(keyMono,
				key -> this.runOnDb(() -> {
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
						if (logger.isTraceEnabled()) {
							logger.trace("Reading {}", LLUtils.toStringSafe(key));
						}
						while (true) {
							@Nullable ByteBuf prevData;
							var prevDataHolder = existsAlmostCertainly ? null : new Holder<byte[]>();
							if (existsAlmostCertainly
									|| db.keyMayExist(cfh, LLUtils.toArray(key), prevDataHolder)) {
								if (!existsAlmostCertainly && prevDataHolder.getValue() != null) {
									byte @Nullable [] prevDataBytes = prevDataHolder.getValue();
									if (prevDataBytes != null) {
										prevData = wrappedBuffer(prevDataBytes);
									} else {
										prevData = null;
									}
								} else {
									prevData = dbGet(cfh, null, key.retain(), existsAlmostCertainly);
								}
							} else {
								prevData = null;
							}
							try {
								@Nullable ByteBuf newData;
								ByteBuf prevDataToSendToUpdater = prevData == null
										? null
										: prevData.retainedSlice();
								try {
									newData = updater.apply(prevDataToSendToUpdater == null
											? null
											: prevDataToSendToUpdater.retain());
									assert !databaseOptions.enableDbAssertionsWhenUsingAssertions()
											|| prevDataToSendToUpdater == null
											|| prevDataToSendToUpdater.readerIndex() == 0
											|| !prevDataToSendToUpdater.isReadable();
								} finally {
									if (prevDataToSendToUpdater != null) {
										prevDataToSendToUpdater.release();
									}
								}
								try {
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
											logger.trace("Deleting {}", LLUtils.toStringSafe(key));
										}
										dbDelete(cfh, null, key.retain());
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
											logger.trace("Writing {}: {}",
													LLUtils.toStringSafe(key), LLUtils.toStringSafe(newData));
										}
										dbPut(cfh, null, key.retain(), newData.retain());
									}
									return new Delta<>(
											prevData != null ? prevData.retain() : null,
											newData != null ? newData.retain() : null
									);
								} finally {
									if (newData != null) {
										newData.release();
									}
								}
							} finally {
								if (prevData != null) {
									prevData.release();
								}
							}
						}
					} finally {
						if (updateMode == UpdateMode.ALLOW) {
							lock.unlock(stamp);
						}
					}
				}).onErrorMap(cause -> new IOException("Failed to read or write "
						+ LLUtils.toStringSafe(key), cause)),
				key -> Mono.fromRunnable(key::release)
		);
	}

	private void dbDelete(ColumnFamilyHandle cfh, @Nullable WriteOptions writeOptions, ByteBuf key)
			throws RocksDBException {
		try {
			var validWriteOptions = Objects.requireNonNullElse(writeOptions, EMPTY_WRITE_OPTIONS);
			if (databaseOptions.allowNettyDirect() && key.isDirect()) {
				if (!key.isDirect()) {
					throw new IllegalArgumentException("Key must be a direct buffer");
				}
				var keyNioBuffer = LLUtils.toDirect(key);
				db.delete(cfh, validWriteOptions, keyNioBuffer);
			} else {
				db.delete(cfh, validWriteOptions, LLUtils.toArray(key));
			}
		} finally {
			key.release();
		}
	}

	@Override
	public Mono<ByteBuf> remove(Mono<ByteBuf> keyMono, LLDictionaryResultType resultType) {
		return Mono.usingWhen(keyMono,
				key -> this
						.getPreviousData(Mono.just(key).map(ByteBuf::retain), resultType)
						.concatWith(this
								.<ByteBuf>runOnDb(() -> {
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
											logger.trace("Deleting {}", LLUtils.toStringSafe(key));
										}
										dbDelete(cfh, null, key.retain());
										return null;
									} finally {
										if (updateMode == UpdateMode.ALLOW) {
											lock.unlockWrite(stamp);
										}
									}
								})
								.onErrorMap(cause -> new IOException("Failed to delete "
										+ LLUtils.toStringSafe(key), cause))
						)
						.singleOrEmpty(),
				key -> Mono.fromCallable(key::release));
	}

	private Mono<ByteBuf> getPreviousData(Mono<ByteBuf> keyMono, LLDictionaryResultType resultType) {
		return Mono
				.usingWhen(keyMono,
						key -> switch (resultType) {
							case PREVIOUS_VALUE_EXISTENCE -> this
									.containsKey(null, Mono.just(key).map(ByteBuf::retain))
									.single()
									.map(LLUtils::booleanToResponseByteBuffer)
									.doAfterTerminate(() -> {
										assert !databaseOptions.enableDbAssertionsWhenUsingAssertions() || key.refCnt() > 0;
									});
							case PREVIOUS_VALUE -> Mono
									.fromCallable(() -> {
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
											if (logger.isTraceEnabled()) {
												logger.trace("Reading {}", LLUtils.toArray(key));
											}
											var data = new Holder<byte[]>();
											if (db.keyMayExist(cfh, LLUtils.toArray(key), data)) {
												if (data.getValue() != null) {
													return wrappedBuffer(data.getValue());
												} else {
													try {
														return dbGet(cfh, null, key.retain(), true);
													} finally {
														assert !databaseOptions.enableDbAssertionsWhenUsingAssertions() || key.refCnt() > 0;
													}
												}
											} else {
												return null;
											}
										} finally {
											if (updateMode == UpdateMode.ALLOW) {
												lock.unlockRead(stamp);
											}
										}
									})
									.onErrorMap(cause -> new IOException("Failed to read " + LLUtils.toStringSafe(key), cause))
									.subscribeOn(dbScheduler);
							case VOID -> Mono.empty();
						},
						key -> Mono.fromRunnable(key::release)
				);
	}

	@Override
	public <K> Flux<Tuple3<K, ByteBuf, Optional<ByteBuf>>> getMulti(@Nullable LLSnapshot snapshot,
			Flux<Tuple2<K, ByteBuf>> keys,
			boolean existsAlmostCertainly) {
		return keys
				.transform(normal -> new BufferTimeOutPublisher<>(normal, MULTI_GET_WINDOW, MULTI_GET_WINDOW_TIMEOUT))
				.doOnDiscard(Tuple2.class, discardedEntry -> {
					//noinspection unchecked
					var entry = (Tuple2<K, ByteBuf>) discardedEntry;
					entry.getT2().release();
				})
				.doOnDiscard(Tuple3.class, discardedEntry -> {
					//noinspection unchecked
					var entry = (Tuple3<K, ByteBuf, ByteBuf>) discardedEntry;
					entry.getT2().release();
					entry.getT3().release();
				})
				.flatMapSequential(keysWindow -> {
					List<ByteBuf> keyBufsWindow = new ArrayList<>(keysWindow.size());
					for (Tuple2<K, ByteBuf> objects : keysWindow) {
						keyBufsWindow.add(objects.getT2());
					}
					return Mono
							.fromCallable(() -> {
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
									var results = db.multiGetAsList(resolveSnapshot(snapshot), columnFamilyHandles, LLUtils.toArray(keyBufsWindow));
									var mappedResults = new ArrayList<Tuple3<K, ByteBuf, Optional<ByteBuf>>>(results.size());
									for (int i = 0; i < results.size(); i++) {
										byte[] val = results.get(i);
										Optional<ByteBuf> valueOpt;
										if (val != null) {
											results.set(i, null);
											valueOpt = Optional.of(wrappedBuffer(val));
										} else {
											valueOpt = Optional.empty();
										}
										mappedResults.add(Tuples.of(keysWindow.get(i).getT1(),
												keyBufsWindow.get(i).retain(),
												valueOpt
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
							})
							.subscribeOn(dbScheduler)
							.flatMapIterable(list -> list)
							.onErrorMap(cause -> new IOException("Failed to read keys "
									+ Arrays.deepToString(keyBufsWindow.toArray(ByteBuf[]::new)), cause))
							.doAfterTerminate(() -> keyBufsWindow.forEach(ReferenceCounted::release));
				}, 2) // Max concurrency is 2 to read data while preparing the next segment
				.doOnDiscard(Entry.class, discardedEntry -> {
					//noinspection unchecked
					var entry = (Entry<ByteBuf, ByteBuf>) discardedEntry;
					entry.getKey().release();
					entry.getValue().release();
				})
				.doOnDiscard(Tuple3.class, discardedEntry -> {
					//noinspection unchecked
					var entry = (Tuple3<K, ByteBuf, Optional<ByteBuf>>) discardedEntry;
					entry.getT2().release();
					entry.getT3().ifPresent(ReferenceCounted::release);
				});
	}

	@Override
	public Flux<Entry<ByteBuf, ByteBuf>> putMulti(Flux<Entry<ByteBuf, ByteBuf>> entries, boolean getOldValues) {
		return entries
				.buffer(Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.flatMapSequential(ew -> Mono
						.using(
								() -> ew,
								entriesWindow -> Mono
										.<Entry<ByteBuf, ByteBuf>>fromCallable(() -> {
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
												if (USE_WRITE_BATCHES_IN_PUT_MULTI) {
													var batch = new CappedWriteBatch(db,
															CAPPED_WRITE_BATCH_CAP,
															RESERVED_WRITE_BATCH_SIZE,
															MAX_WRITE_BATCH_SIZE,
															BATCH_WRITE_OPTIONS
													);
													for (Entry<ByteBuf, ByteBuf> entry : entriesWindow) {
														batch.put(cfh, entry.getKey().retain(), entry.getValue().retain());
													}
													batch.writeToDbAndClose();
													batch.close();
												} else {
													for (Entry<ByteBuf, ByteBuf> entry : entriesWindow) {
														db.put(cfh, EMPTY_WRITE_OPTIONS, entry.getKey().nioBuffer(), entry.getValue().nioBuffer());
													}
												}
												return null;
											} finally {
												if (updateMode == UpdateMode.ALLOW) {
													int index = 0;
													for (var lock : locks) {
														lock.unlockWrite(stamps.get(index));
														index++;
													}
												}
											}
										})
										.subscribeOn(dbScheduler)

										// Prepend everything to get previous elements
										.transform(transformer -> {
											var obj = new Object();
											if (getOldValues) {
												return this
														.getMulti(null, Flux
																.fromIterable(entriesWindow)
																.map(Entry::getKey)
																.map(ByteBuf::retain)
																.map(buf -> Tuples.of(obj, buf)), false)
														.publishOn(dbScheduler)
														.then(transformer);
											} else {
												return transformer;
											}
										}),
								entriesWindow -> {
									for (Entry<ByteBuf, ByteBuf> entry : entriesWindow) {
										entry.getKey().release();
										entry.getValue().release();
									}
								}
						), 2) // Max concurrency is 2 to read data while preparing the next segment
				.transform(LLUtils::handleDiscard);
	}

	@Override
	public <X> Flux<ExtraKeyOperationResult<ByteBuf, X>> updateMulti(Flux<Tuple2<ByteBuf, X>> entries,
			BiSerializationFunction<ByteBuf, X, ByteBuf> updateFunction) {
		return entries
				.buffer(Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.flatMapSequential(ew -> Flux
						.using(
								() -> ew,
								entriesWindow -> {
									List<ByteBuf> keyBufsWindow = new ArrayList<>(entriesWindow.size());
									for (Tuple2<ByteBuf, X> objects : entriesWindow) {
										keyBufsWindow.add(objects.getT1());
									}
									return Mono
											.<Iterable<ExtraKeyOperationResult<ByteBuf, X>>>fromCallable(() -> {
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
													ArrayList<Tuple3<ByteBuf, X, Optional<ByteBuf>>> mappedInputs;
													{
														var inputs = db.multiGetAsList(resolveSnapshot(null), columnFamilyHandles, LLUtils.toArray(keyBufsWindow));
														mappedInputs = new ArrayList<>(inputs.size());
														for (int i = 0; i < inputs.size(); i++) {
															var val = inputs.get(i);
															if (val != null) {
																inputs.set(i, null);
																mappedInputs.add(Tuples.of(
																		keyBufsWindow.get(i).retain(),
																		entriesWindow.get(i).getT2(),
																		Optional.of(wrappedBuffer(val))
																));
															} else {
																mappedInputs.add(Tuples.of(
																		keyBufsWindow.get(i).retain(),
																		entriesWindow.get(i).getT2(),
																		Optional.empty()
																));
															}
														}
													}
													var updatedValuesToWrite = new ArrayList<ByteBuf>(mappedInputs.size());
													var valueChangedResult = new ArrayList<ExtraKeyOperationResult<ByteBuf, X>>(mappedInputs.size());
													try {
														for (var mappedInput : mappedInputs) {
															var updatedValue = updateFunction.apply(mappedInput.getT1().retain(), mappedInput.getT2());
															valueChangedResult.add(new ExtraKeyOperationResult<>(mappedInput.getT1(),
																	mappedInput.getT2(),
																	!Objects.equals(mappedInput.getT3().orElse(null), updatedValue.retain())
															));
															updatedValuesToWrite.add(updatedValue);
														}
													} finally {
														for (var mappedInput : mappedInputs) {
															mappedInput.getT3().ifPresent(ReferenceCounted::release);
														}
													}

													if (USE_WRITE_BATCHES_IN_PUT_MULTI) {
														var batch = new CappedWriteBatch(db,
																CAPPED_WRITE_BATCH_CAP,
																RESERVED_WRITE_BATCH_SIZE,
																MAX_WRITE_BATCH_SIZE,
																BATCH_WRITE_OPTIONS
														);
														int i = 0;
														for (Tuple2<ByteBuf, X> entry : entriesWindow) {
															var valueToWrite = updatedValuesToWrite.get(i);
															if (valueToWrite == null) {
																batch.delete(cfh, entry.getT1().retain());
															} else {
																batch.put(cfh, entry.getT1().retain(), valueToWrite.retain());
															}
															i++;
														}
														batch.writeToDbAndClose();
														batch.close();
													} else {
														int i = 0;
														for (Tuple2<ByteBuf, X> entry : entriesWindow) {
															var valueToWrite = updatedValuesToWrite.get(i);
															db.put(cfh, EMPTY_WRITE_OPTIONS, entry.getT1().nioBuffer(), valueToWrite.nioBuffer());
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
											})
											.subscribeOn(dbScheduler)
											.flatMapIterable(list -> list);
								},
								entriesWindow -> {
									for (Tuple2<ByteBuf, X> entry : entriesWindow) {
										entry.getT1().release();
									}
								}
						), 2 // Max concurrency is 2 to update data while preparing the next segment
				)
				.doOnDiscard(Tuple2.class, entry -> {
					if (entry.getT1() instanceof ByteBuf bb) {
						bb.release();
					}
					if (entry.getT2() instanceof ByteBuf bb) {
						bb.release();
					}
				})
				.doOnDiscard(ExtraKeyOperationResult.class, entry -> {
					if (entry.key() instanceof ByteBuf bb) {
						bb.release();
					}
					if (entry.extra() instanceof ByteBuf bb) {
						bb.release();
					}
				})
				.doOnDiscard(Collection.class, obj -> {
					//noinspection unchecked
					var castedEntries = (Collection<ExtraKeyOperationResult<Object, Object>>) obj;
					for (var entry : castedEntries) {
						if (entry.key() instanceof ByteBuf bb) {
							bb.release();
						}
						if (entry.extra() instanceof ByteBuf bb) {
							bb.release();
						}
					}
				});
	}

	@Override
	public Flux<Entry<ByteBuf, ByteBuf>> getRange(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			boolean existsAlmostCertainly) {
		return Flux.usingWhen(rangeMono,
				range -> {
					if (range.isSingle()) {
						return getRangeSingle(snapshot, Mono.just(range.getMin()).map(ByteBuf::retain), existsAlmostCertainly);
					} else {
						return getRangeMulti(snapshot, Mono.just(range).map(LLRange::retain));
					}
				},
				range -> Mono.fromRunnable(range::release)
		);
	}

	@Override
	public Flux<List<Entry<ByteBuf, ByteBuf>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			int prefixLength, boolean existsAlmostCertainly) {
		return Flux.usingWhen(rangeMono,
				range -> {
					if (range.isSingle()) {
						var rangeSingleMono = Mono.just(range.getMin()).map(ByteBuf::retain);
						return getRangeSingle(snapshot, rangeSingleMono, existsAlmostCertainly).map(List::of);
					} else {
						return getRangeMultiGrouped(snapshot, Mono.just(range).map(LLRange::retain), prefixLength);
					}
				},
				range -> Mono.fromRunnable(range::release)
		);
	}

	private Flux<Entry<ByteBuf, ByteBuf>> getRangeSingle(LLSnapshot snapshot,
			Mono<ByteBuf> keyMono,
			boolean existsAlmostCertainly) {
		return Flux.usingWhen(keyMono,
				key -> this
						.get(snapshot, Mono.just(key).map(ByteBuf::retain), existsAlmostCertainly)
						.map(value -> Map.entry(key.retain(), value)),
				key -> Mono.fromRunnable(key::release)
		);
	}

	private Flux<Entry<ByteBuf, ByteBuf>> getRangeMulti(LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return Flux.usingWhen(rangeMono,
				range -> Flux
						.using(
								() -> new LLLocalEntryReactiveRocksIterator(db,
										alloc,
										cfh,
										range.retain(),
										databaseOptions.allowNettyDirect(),
										resolveSnapshot(snapshot),
										getRangeMultiDebugName
								),
								llLocalEntryReactiveRocksIterator -> llLocalEntryReactiveRocksIterator
										.flux()
										.subscribeOn(dbScheduler),
								LLLocalReactiveRocksIterator::release
						),
				range -> Mono.fromRunnable(range::release)
		);
	}

	private Flux<List<Entry<ByteBuf, ByteBuf>>> getRangeMultiGrouped(LLSnapshot snapshot, Mono<LLRange> rangeMono, int prefixLength) {
		return Flux.usingWhen(rangeMono,
				range -> Flux
						.using(
								() -> new LLLocalGroupedEntryReactiveRocksIterator(db,
										alloc,
										cfh,
										prefixLength,
										range.retain(),
										databaseOptions.allowNettyDirect(),
										resolveSnapshot(snapshot),
										"getRangeMultiGrouped"
								),
								reactiveRocksIterator -> reactiveRocksIterator
										.flux()
										.subscribeOn(dbScheduler),
								LLLocalGroupedReactiveRocksIterator::release
						),
				range -> Mono.fromRunnable(range::release)
		);
	}

	@Override
	public Flux<ByteBuf> getRangeKeys(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return Flux.usingWhen(rangeMono,
				range -> {
					if (range.isSingle()) {
						return this.getRangeKeysSingle(snapshot, Mono.just(range.getMin()).map(ByteBuf::retain));
					} else {
						return this.getRangeKeysMulti(snapshot, Mono.just(range).map(LLRange::retain));
					}
				},
				range -> Mono.fromRunnable(range::release)
		);
	}

	@Override
	public Flux<List<ByteBuf>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			Mono<LLRange> rangeMono,
			int prefixLength) {
		return Flux.usingWhen(rangeMono,
				range -> Flux
						.using(
								() -> new LLLocalGroupedKeyReactiveRocksIterator(db,
										alloc,
										cfh,
										prefixLength,
										range.retain(),
										databaseOptions.allowNettyDirect(),
										resolveSnapshot(snapshot),
										"getRangeKeysGrouped"
								), reactiveRocksIterator -> reactiveRocksIterator.flux()
										.subscribeOn(dbScheduler),
								LLLocalGroupedReactiveRocksIterator::release
						),
				range -> Mono.fromRunnable(range::release)
		);
	}

	@Override
	public Flux<BadBlock> badBlocks(Mono<LLRange> rangeMono) {
		return Flux.usingWhen(rangeMono,
				range -> Flux
						.<BadBlock>create(sink -> {
							try (var ro = new ReadOptions(getReadOptions(null))) {
								ro.setFillCache(false);
								if (!range.isSingle()) {
									ro.setReadaheadSize(32 * 1024);
								}
								ro.setVerifyChecksums(true);
								var rocksIteratorTuple = getRocksIterator(databaseOptions.allowNettyDirect(), ro, range.retain(), db, cfh);
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
									rocksIteratorTuple.getT2().release();
									rocksIteratorTuple.getT3().release();
								}
								sink.complete();
							} catch (Throwable ex) {
								sink.error(ex);
							}
						})
						.subscribeOn(dbScheduler),
				range -> Mono.fromRunnable(range::release)
		);
	}

	@Override
	public Flux<ByteBuf> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono, int prefixLength) {
		return Flux.usingWhen(rangeMono,
				range -> Flux
						.using(
								() -> new LLLocalKeyPrefixReactiveRocksIterator(db,
										alloc,
										cfh,
										prefixLength,
										range.retain(),
										databaseOptions.allowNettyDirect(),
										resolveSnapshot(snapshot),
										true,
										"getRangeKeysGrouped"
								),
								LLLocalKeyPrefixReactiveRocksIterator::flux,
								LLLocalKeyPrefixReactiveRocksIterator::release
						)
						.subscribeOn(dbScheduler),
				range -> Mono.fromRunnable(range::release)
		);
	}

	private Flux<ByteBuf> getRangeKeysSingle(LLSnapshot snapshot, Mono<ByteBuf> keyMono) {
		return Flux.usingWhen(keyMono,
				key -> this
						.containsKey(snapshot, Mono.just(key).map(ByteBuf::retain))
						.flux()
						.<ByteBuf>handle((contains, sink) -> {
							if (contains) {
								sink.next(key.retain());
							} else {
								sink.complete();
							}
						})
						.doOnDiscard(ByteBuf.class, ReferenceCounted::release),
				key -> Mono.fromRunnable(key::release)
		);
	}

	@SuppressWarnings("Convert2MethodRef")
	private Flux<ByteBuf> getRangeKeysMulti(LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return Flux.usingWhen(rangeMono,
				range -> Flux
						.using(
								() -> new LLLocalKeyReactiveRocksIterator(db,
										alloc,
										cfh,
										range.retain(),
										databaseOptions.allowNettyDirect(),
										resolveSnapshot(snapshot),
										getRangeKeysMultiDebugName
								),
								llLocalKeyReactiveRocksIterator -> llLocalKeyReactiveRocksIterator.flux(),
								LLLocalReactiveRocksIterator::release
						)
						.doOnDiscard(ByteBuf.class, ReferenceCounted::release)
						.subscribeOn(dbScheduler),
				range -> Mono.fromRunnable(range::release)
		);
	}

	@Override
	public Mono<Void> setRange(Mono<LLRange> rangeMono, Flux<Entry<ByteBuf, ByteBuf>> entries) {
		return Mono.usingWhen(rangeMono,
				range -> {
					if (USE_WINDOW_IN_SET_RANGE) {
						return Mono
								.<Void>fromCallable(() -> {
									if (!USE_WRITE_BATCH_IN_SET_RANGE_DELETE || !USE_WRITE_BATCHES_IN_SET_RANGE) {
										assert EMPTY_READ_OPTIONS.isOwningHandle();
										try (var opts = new ReadOptions(EMPTY_READ_OPTIONS)) {
											ReleasableSlice minBound;
											if (range.hasMin()) {
												minBound = setIterateBound(databaseOptions.allowNettyDirect(),
														opts,
														IterateBound.LOWER,
														range.getMin().retain()
												);
											} else {
												minBound = emptyReleasableSlice();
											}
											try {
												ReleasableSlice maxBound;
												if (range.hasMax()) {
													maxBound = setIterateBound(databaseOptions.allowNettyDirect(),
															opts,
															IterateBound.UPPER,
															range.getMax().retain()
													);
												} else {
													maxBound = emptyReleasableSlice();
												}
												assert cfh.isOwningHandle();
												assert opts.isOwningHandle();
												try (RocksIterator it = db.newIterator(cfh, opts)) {
													if (!PREFER_SEEK_TO_FIRST && range.hasMin()) {
														rocksIterSeekTo(databaseOptions.allowNettyDirect(), it, range.getMin().retain());
													} else {
														it.seekToFirst();
													}
													it.status();
													while (it.isValid()) {
														db.delete(cfh, it.key());
														it.next();
														it.status();
													}
												} finally {
													maxBound.release();
												}
											} finally {
												minBound.release();
											}
										}
									} else if (USE_CAPPED_WRITE_BATCH_IN_SET_RANGE) {
										try (var batch = new CappedWriteBatch(db,
												CAPPED_WRITE_BATCH_CAP,
												RESERVED_WRITE_BATCH_SIZE,
												MAX_WRITE_BATCH_SIZE,
												BATCH_WRITE_OPTIONS
										)) {
											if (range.isSingle()) {
												batch.delete(cfh, range.getSingle().retain());
											} else {
												deleteSmallRangeWriteBatch(batch, range.retain());
											}
											batch.writeToDbAndClose();
										}
									} else {
										try (var batch = new WriteBatch(RESERVED_WRITE_BATCH_SIZE)) {
											if (range.isSingle()) {
												batch.delete(cfh, LLUtils.toArray(range.getSingle()));
											} else {
												deleteSmallRangeWriteBatch(batch, range.retain());
											}
											db.write(EMPTY_WRITE_OPTIONS, batch);
											batch.clear();
										}
									}
									return null;
								})
								.subscribeOn(dbScheduler)
								.thenMany(entries
										.window(MULTI_GET_WINDOW)
								)
								.flatMap(keysWindowFlux -> keysWindowFlux
										.collectList()
										.doOnDiscard(Entry.class, discardedEntry -> {
											//noinspection unchecked
											var entry = (Entry<ByteBuf, ByteBuf>) discardedEntry;
											entry.getKey().release();
											entry.getValue().release();
										})
										.flatMap(entriesList -> Mono
												.<Void>fromCallable(() -> {
													try {
														if (!USE_WRITE_BATCHES_IN_SET_RANGE) {
															for (Entry<ByteBuf, ByteBuf> entry : entriesList) {
																db.put(cfh, EMPTY_WRITE_OPTIONS, entry.getKey().nioBuffer(), entry.getValue().nioBuffer());
															}
														} else if (USE_CAPPED_WRITE_BATCH_IN_SET_RANGE) {
															try (var batch = new CappedWriteBatch(db,
																	CAPPED_WRITE_BATCH_CAP,
																	RESERVED_WRITE_BATCH_SIZE,
																	MAX_WRITE_BATCH_SIZE,
																	BATCH_WRITE_OPTIONS
															)) {
																for (Entry<ByteBuf, ByteBuf> entry : entriesList) {
																	batch.put(cfh, entry.getKey().retain(), entry.getValue().retain());
																}
																batch.writeToDbAndClose();
															}
														} else {
															try (var batch = new WriteBatch(RESERVED_WRITE_BATCH_SIZE)) {
																for (Entry<ByteBuf, ByteBuf> entry : entriesList) {
																	batch.put(cfh, LLUtils.toArray(entry.getKey()), LLUtils.toArray(entry.getValue()));
																}
																db.write(EMPTY_WRITE_OPTIONS, batch);
																batch.clear();
															}
														}
														return null;
													} finally {
														for (Entry<ByteBuf, ByteBuf> entry : entriesList) {
															entry.getKey().release();
															entry.getValue().release();
														}
													}
												})
												.subscribeOn(dbScheduler)
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
								.getRange(null, Mono.just(range).map(LLRange::retain), false)
								.flatMap(oldValue -> Mono
										.<Void>fromCallable(() -> {
											try {
												dbDelete(cfh, EMPTY_WRITE_OPTIONS, oldValue.getKey().retain());
												return null;
											} finally {
												oldValue.getKey().release();
												oldValue.getValue().release();
											}
										})
										.subscribeOn(dbScheduler)
								)
								.then(entries
										.flatMap(entry -> Mono.using(
												() -> entry,
												releasableEntry -> this
														.put(Mono.just(entry.getKey()).map(ByteBuf::retain),
																Mono.just(entry.getValue()).map(ByteBuf::retain),
																LLDictionaryResultType.VOID
														)
														.doOnNext(ReferenceCounted::release),
												releasableEntry -> {
													releasableEntry.getKey().release();
													releasableEntry.getValue().release();
												})
										)
										.then(Mono.<Void>empty())
								)
								.onErrorMap(cause -> new IOException("Failed to write range", cause));
					}
				},
				range -> Mono.fromRunnable(range::release)
		);
	}

	//todo: this is broken, check why. (is this still true?)
	private void deleteSmallRangeWriteBatch(CappedWriteBatch writeBatch, LLRange range)
			throws RocksDBException {
		try (var readOpts = new ReadOptions(getReadOptions(null))) {
			readOpts.setFillCache(false);
			ReleasableSlice minBound;
			if (range.hasMin()) {
				minBound = setIterateBound(databaseOptions.allowNettyDirect(),
						readOpts,
						IterateBound.LOWER,
						range.getMin().retain()
				);
			} else {
				minBound = emptyReleasableSlice();
			}
			try {
				ReleasableSlice maxBound;
				if (range.hasMax()) {
					maxBound = setIterateBound(databaseOptions.allowNettyDirect(),
							readOpts,
							IterateBound.UPPER,
							range.getMax().retain()
					);
				} else {
					maxBound = emptyReleasableSlice();
				}
				try (var rocksIterator = db.newIterator(cfh, readOpts)) {
					if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
						rocksIterSeekTo(databaseOptions.allowNettyDirect(), rocksIterator, range.getMin().retain());
					} else {
						rocksIterator.seekToFirst();
					}
					rocksIterator.status();
					while (rocksIterator.isValid()) {
						writeBatch.delete(cfh, LLUtils.readDirectNioBuffer(alloc, rocksIterator::key));
						rocksIterator.next();
						rocksIterator.status();
					}
				} finally {
					maxBound.release();
				}
			} finally {
				minBound.release();
			}
		} finally {
			range.release();
		}
	}

	private void deleteSmallRangeWriteBatch(WriteBatch writeBatch, LLRange range)
			throws RocksDBException {
		try (var readOpts = new ReadOptions(getReadOptions(null))) {
			readOpts.setFillCache(false);
			ReleasableSlice minBound;
			if (range.hasMin()) {
				minBound = setIterateBound(databaseOptions.allowNettyDirect(),
						readOpts,
						IterateBound.LOWER,
						range.getMin().retain()
				);
			} else {
				minBound = emptyReleasableSlice();
			}
			try {
				ReleasableSlice maxBound;
				if (range.hasMax()) {
					maxBound = setIterateBound(databaseOptions.allowNettyDirect(),
							readOpts,
							IterateBound.UPPER,
							range.getMax().retain()
					);
				} else {
					maxBound = emptyReleasableSlice();
				}
				try (var rocksIterator = db.newIterator(cfh, readOpts)) {
					if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
						rocksIterSeekTo(databaseOptions.allowNettyDirect(), rocksIterator, range.getMin().retain());
					} else {
						rocksIterator.seekToFirst();
					}
					rocksIterator.status();
					while (rocksIterator.isValid()) {
						writeBatch.delete(cfh, rocksIterator.key());
						rocksIterator.next();
						rocksIterator.status();
					}
				} finally {
					maxBound.release();
				}
			} finally {
				minBound.release();
			}
		} finally {
			range.release();
		}
	}

	private static void rocksIterSeekTo(boolean allowNettyDirect, RocksIterator rocksIterator, ByteBuf buffer) {
		try {
			if (allowNettyDirect && buffer.isDirect()) {
				ByteBuffer nioBuffer = LLUtils.toDirect(buffer);
				assert nioBuffer.isDirect();
				rocksIterator.seek(nioBuffer);
			} else if (buffer.hasArray() && buffer.array().length == buffer.readableBytes()) {
				rocksIterator.seek(buffer.array());
			} else {
				rocksIterator.seek(LLUtils.toArray(buffer));
			}
		} finally {
			buffer.release();
		}
	}

	private static ReleasableSlice setIterateBound(boolean allowNettyDirect, ReadOptions readOpts, IterateBound boundType, ByteBuf buffer) {
		try {
			Objects.requireNonNull(buffer);
			AbstractSlice<?> slice;
			if (allowNettyDirect && LLLocalDictionary.USE_DIRECT_BUFFER_BOUNDS && buffer.isDirect()) {
				ByteBuffer nioBuffer = LLUtils.toDirect(buffer);
				assert nioBuffer.isDirect();
				slice = new DirectSlice(nioBuffer, buffer.readableBytes());
				assert slice.size() == buffer.readableBytes();
				assert slice.compare(new Slice(LLUtils.toArray(buffer))) == 0;
				if (boundType == IterateBound.LOWER) {
					readOpts.setIterateLowerBound(slice);
				} else {
					readOpts.setIterateUpperBound(slice);
				}
				return new ReleasableSliceImpl(slice, buffer.retain(), nioBuffer);
			} else {
				slice = new Slice(Objects.requireNonNull(LLUtils.toArray(buffer)));
				if (boundType == IterateBound.LOWER) {
					readOpts.setIterateLowerBound(slice);
				} else {
					readOpts.setIterateUpperBound(slice);
				}
				return new ReleasableSliceImpl(slice, null, null);
			}
		} finally {
			buffer.release();
		}
	}

	private static ReleasableSlice emptyReleasableSlice() {
		var arr = new byte[0];

		return new SimpleSliceWithoutRelease(new Slice(arr), null, arr);
	}

	public static record SimpleSliceWithoutRelease(AbstractSlice<?> slice, @Nullable ByteBuf byteBuf,
																								 @Nullable Object additionalData) implements ReleasableSlice {}

	public static record ReleasableSliceImpl(AbstractSlice<?> slice, @Nullable ByteBuf byteBuf,
																					 @Nullable Object additionalData) implements ReleasableSlice {

		@Override
		public void release() {
			slice.clear();
			if (byteBuf != null) {
				byteBuf.release();
			}
		}
	}

	public Mono<Void> clear() {
		return Mono
				.<Void>fromCallable(() -> {
					try (var readOpts = new ReadOptions(getReadOptions(null))) {
						readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);

						// readOpts.setIgnoreRangeDeletions(true);
						readOpts.setFillCache(false);
						readOpts.setReadaheadSize(32 * 1024); // 32KiB
						try (CappedWriteBatch writeBatch = new CappedWriteBatch(db,
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
	public Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono, boolean fast) {
		return Mono.usingWhen(rangeMono,
				range -> {
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
									minBound = setIterateBound(databaseOptions.allowNettyDirect(),
											readOpts,
											IterateBound.LOWER,
											range.getMin().retain()
									);
								} else {
									minBound = emptyReleasableSlice();
								}
								try {
									ReleasableSlice maxBound;
									if (range.hasMax()) {
										maxBound = setIterateBound(databaseOptions.allowNettyDirect(),
												readOpts,
												IterateBound.UPPER,
												range.getMax().retain()
										);
									} else {
										maxBound = emptyReleasableSlice();
									}
									try {
										if (fast) {
											readOpts.setIgnoreRangeDeletions(true);

										}
										try (var rocksIterator = db.newIterator(cfh, readOpts)) {
											if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
												rocksIterSeekTo(databaseOptions.allowNettyDirect(),
														rocksIterator,
														range.getMin().retain()
												);
											} else {
												rocksIterator.seekToFirst();
											}
											long i = 0;
											rocksIterator.status();
											while (rocksIterator.isValid()) {
												rocksIterator.next();
												rocksIterator.status();
												i++;
											}
											return i;
										}
									} finally {
										maxBound.release();
									}
								} finally {
									minBound.release();
								}
							}
						}).onErrorMap(cause -> new IOException("Failed to get size of range " + range, cause));
					}
				},
				range -> Mono.fromRunnable(range::release)
		);
	}

	@Override
	public Mono<Entry<ByteBuf, ByteBuf>> getOne(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return Mono.usingWhen(rangeMono,
				range -> runOnDb(() -> {
					try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
						ReleasableSlice minBound;
						if (range.hasMin()) {
							minBound = setIterateBound(databaseOptions.allowNettyDirect(),
									readOpts,
									IterateBound.LOWER,
									range.getMin().retain()
							);
						} else {
							minBound = emptyReleasableSlice();
						}
						try {
							ReleasableSlice maxBound;
							if (range.hasMax()) {
								maxBound = setIterateBound(databaseOptions.allowNettyDirect(),
										readOpts,
										IterateBound.UPPER,
										range.getMax().retain()
								);
							} else {
								maxBound = emptyReleasableSlice();
							}
							try (var rocksIterator = db.newIterator(cfh, readOpts)) {
								if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
									rocksIterSeekTo(databaseOptions.allowNettyDirect(), rocksIterator, range.getMin().retain());
								} else {
									rocksIterator.seekToFirst();
								}
								rocksIterator.status();
								if (rocksIterator.isValid()) {
									ByteBuf key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
									try {
										ByteBuf value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value);
										try {
											return Map.entry(key.retain(), value.retain());
										} finally {
											value.release();
										}
									} finally {
										key.release();
									}
								} else {
									return null;
								}
							} finally {
								maxBound.release();
							}
						} finally {
							minBound.release();
						}
					}
				}),
				range -> Mono.fromRunnable(range::release)
		);
	}

	@Override
	public Mono<ByteBuf> getOneKey(@Nullable LLSnapshot snapshot, Mono<LLRange> rangeMono) {
		return Mono.usingWhen(rangeMono,
				range -> runOnDb(() -> {
					try (var readOpts = new ReadOptions(resolveSnapshot(snapshot))) {
						ReleasableSlice minBound;
						if (range.hasMin()) {
							minBound = setIterateBound(databaseOptions.allowNettyDirect(),
									readOpts,
									IterateBound.LOWER,
									range.getMin().retain()
							);
						} else {
							minBound = emptyReleasableSlice();
						}
						try {
							ReleasableSlice maxBound;
							if (range.hasMax()) {
								maxBound = setIterateBound(databaseOptions.allowNettyDirect(),
										readOpts,
										IterateBound.UPPER,
										range.getMax().retain()
								);
							} else {
								maxBound = emptyReleasableSlice();
							}
							try (var rocksIterator = db.newIterator(cfh, readOpts)) {
								if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
									rocksIterSeekTo(databaseOptions.allowNettyDirect(), rocksIterator, range.getMin().retain());
								} else {
									rocksIterator.seekToFirst();
								}
								ByteBuf key;
								rocksIterator.status();
								if (rocksIterator.isValid()) {
									key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
									return key;
								} else {
									return null;
								}
							} finally {
								maxBound.release();
							}
						} finally {
							minBound.release();
						}
					}
				}),
				range -> Mono.fromRunnable(range::release)
		);
	}

	private long fastSizeAll(@Nullable LLSnapshot snapshot) throws RocksDBException {
		try (var rocksdbSnapshot = new ReadOptions(resolveSnapshot(snapshot))) {
			if (USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS || rocksdbSnapshot.snapshot() == null) {
				try {
					return db.getLongProperty(cfh, "rocksdb.estimate-num-keys");
				} catch (RocksDBException e) {
					e.printStackTrace();
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
	public Mono<Entry<ByteBuf, ByteBuf>> removeOne(Mono<LLRange> rangeMono) {
		return Mono.usingWhen(rangeMono,
				range -> runOnDb(() -> {
					try (var readOpts = new ReadOptions(getReadOptions(null))) {
						ReleasableSlice minBound;
						if (range.hasMin()) {
							minBound = setIterateBound(databaseOptions.allowNettyDirect(),
									readOpts,
									IterateBound.LOWER,
									range.getMin().retain()
							);
						} else {
							minBound = emptyReleasableSlice();
						}
						try {
							ReleasableSlice maxBound;
							if (range.hasMax()) {
								maxBound = setIterateBound(databaseOptions.allowNettyDirect(),
										readOpts,
										IterateBound.UPPER,
										range.getMax().retain()
								);
							} else {
								maxBound = emptyReleasableSlice();
							}
							try (RocksIterator rocksIterator = db.newIterator(cfh, readOpts)) {
								if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
									rocksIterSeekTo(databaseOptions.allowNettyDirect(), rocksIterator, range.getMin().retain());
								} else {
									rocksIterator.seekToFirst();
								}
								rocksIterator.status();
								if (!rocksIterator.isValid()) {
									return null;
								}
								ByteBuf key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
								ByteBuf value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value);
								dbDelete(cfh, null, key);
								return Map.entry(key, value);
							} finally {
								maxBound.release();
							}
						} finally {
							minBound.release();
						}
					}
				}).onErrorMap(cause -> new IOException("Failed to delete " + range.toString(), cause)),
				range -> Mono.fromRunnable(range::release)
		);
	}

	@NotNull
	public static Tuple3<RocksIterator, ReleasableSlice, ReleasableSlice> getRocksIterator(boolean allowNettyDirect,
			ReadOptions readOptions,
			LLRange range,
			RocksDB db,
			ColumnFamilyHandle cfh) {
		try {
			ReleasableSlice sliceMin;
			ReleasableSlice sliceMax;
			if (range.hasMin()) {
				sliceMin = setIterateBound(allowNettyDirect, readOptions, IterateBound.LOWER, range.getMin().retain());
			} else {
				sliceMin = emptyReleasableSlice();
			}
			if (range.hasMax()) {
				sliceMax = setIterateBound(allowNettyDirect, readOptions, IterateBound.UPPER, range.getMax().retain());
			} else {
				sliceMax = emptyReleasableSlice();
			}
			var rocksIterator = db.newIterator(cfh, readOptions);
			if (!PREFER_SEEK_TO_FIRST && range.hasMin()) {
				rocksIterSeekTo(allowNettyDirect, rocksIterator, range.getMin().retain());
			} else {
				rocksIterator.seekToFirst();
			}
			return Tuples.of(rocksIterator, sliceMin, sliceMax);
		} finally {
			range.release();
		}
	}
}
