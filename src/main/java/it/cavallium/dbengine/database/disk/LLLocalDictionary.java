package it.cavallium.dbengine.database.disk;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Data;
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
import org.rocksdb.WriteOptions;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;
import org.warp.commonutils.locks.Striped;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;
import static io.netty.buffer.Unpooled.*;

@NotAtomic
public class LLLocalDictionary implements LLDictionary {

	protected static final Logger logger = LoggerFactory.getLogger(LLLocalDictionary.class);
	private static final boolean USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS = false;
	static final int RESERVED_WRITE_BATCH_SIZE = 2 * 1024 * 1024; // 2MiB
	static final long MAX_WRITE_BATCH_SIZE = 1024L * 1024L * 1024L; // 1GiB
	static final int CAPPED_WRITE_BATCH_CAP = 50000; // 50K operations
	static final int MULTI_GET_WINDOW = 500;
	static final ReadOptions EMPTY_READ_OPTIONS = new ReadOptions();
	static final WriteOptions EMPTY_WRITE_OPTIONS = new WriteOptions();
	static final WriteOptions BATCH_WRITE_OPTIONS = new WriteOptions().setLowPri(true);
	static final boolean PREFER_SEEK_TO_FIRST = false;
	static final boolean VERIFY_CHECKSUMS_WHEN_NOT_NEEDED = false;
	public static final boolean DEBUG_PREFIXES_WHEN_ASSERTIONS_ARE_ENABLED = true;
	static final boolean PARALLEL_EXACT_SIZE = true;

	private static final int STRIPES = 512;
	private static final byte[] FIRST_KEY = new byte[]{};
	private static final byte[] NO_DATA = new byte[0];

	private static final boolean ASSERTIONS_ENABLED;

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
	private final Scheduler dbScheduler;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;
	private final Striped<StampedLock> itemsLock = Striped.readWriteStampedLock(STRIPES);
	private final UpdateMode updateMode;
	private final ByteBufAllocator alloc;

	public LLLocalDictionary(@NotNull RocksDB db,
			@NotNull ColumnFamilyHandle columnFamilyHandle,
			String databaseName,
			Scheduler dbScheduler,
			Function<LLSnapshot, Snapshot> snapshotResolver,
			UpdateMode updateMode) {
		Objects.requireNonNull(db);
		this.db = db;
		Objects.requireNonNull(columnFamilyHandle);
		this.cfh = columnFamilyHandle;
		this.databaseName = databaseName;
		this.dbScheduler = dbScheduler;
		this.snapshotResolver = snapshotResolver;
		this.updateMode = updateMode;
		alloc = PooledByteBufAllocator.DEFAULT;
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}

	private ReadOptions resolveSnapshot(LLSnapshot snapshot) {
		if (snapshot != null) {
			return getReadOptions(snapshotResolver.apply(snapshot));
		} else {
			return EMPTY_READ_OPTIONS;
		}
	}

	private ReadOptions getReadOptions(Snapshot snapshot) {
		if (snapshot != null) {
			return new ReadOptions().setSnapshot(snapshot);
		} else {
			return EMPTY_READ_OPTIONS;
		}
	}

	private int getLockIndex(ByteBuf key) {
		return Math.abs(key.hashCode() % STRIPES);
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

	@Override
	public ByteBufAllocator getAllocator() {
		return alloc;
	}

	@Override
	public Mono<ByteBuf> get(@Nullable LLSnapshot snapshot, ByteBuf key, boolean existsAlmostCertainly) {
		return Mono
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
							logger.trace("Reading {}", LLUtils.toString(key));
						}
						return dbGet(cfh, resolveSnapshot(snapshot), key.retain());
					} finally {
						if (updateMode == UpdateMode.ALLOW) {
							lock.unlockRead(stamp);
						}
					}
				})
				.onErrorMap(cause -> new IOException("Failed to read " + LLUtils.toString(key), cause))
				.subscribeOn(dbScheduler)
				.doFinally(s -> key.release());
	}

	private ByteBuf dbGet(ColumnFamilyHandle cfh, @Nullable ReadOptions readOptions, ByteBuf key) throws RocksDBException {
		//todo: implement keyMayExist if existsAlmostCertainly is false.
		// Unfortunately it's not feasible until RocksDB implements keyMayExist with buffers

		// Create the key nio buffer to pass to RocksDB
		if (!key.isDirect()) {
			throw new RocksDBException("Key buffer must be direct");
		}
		try {
			ByteBuf keyDirectBuf = key.retain();
			ByteBuffer keyNioBuffer = LLUtils.toDirectFast(keyDirectBuf.retain());
			if (keyNioBuffer == null) {
				keyDirectBuf.release();
				keyDirectBuf = LLUtils.toDirectCopy(key.retain());
				keyNioBuffer = keyDirectBuf.nioBuffer();
			}
			try {
				assert keyNioBuffer.isDirect();
				// Create a direct result buffer because RocksDB works only with direct buffers
				ByteBuf resultBuf = alloc.directBuffer();
				try {
					int valueSize;
					int assertionReadData = -1;
					ByteBuffer resultNioBuf;
					do {
						// Create the result nio buffer to pass to RocksDB
						resultNioBuf = resultBuf.nioBuffer(0, resultBuf.capacity());
						assert keyNioBuffer.isDirect();
						assert resultNioBuf.isDirect();
						valueSize = db.get(cfh, Objects.requireNonNullElse(readOptions, EMPTY_READ_OPTIONS), keyNioBuffer, resultNioBuf);
						if (valueSize != RocksDB.NOT_FOUND) {
							// todo: check if position is equal to data that have been read
							// todo: check if limit is equal to value size or data that have been read
							assert valueSize <= 0 || resultNioBuf.limit() > 0;

							// If the locking is enabled the data is safe, so since we are appending data to the end,
							// we need to check if it has been appended correctly or it it has been overwritten.
							// We must not do this check otherwise because if there is no locking the data can be
							// overwritten with a smaller value the next time.
							if (updateMode == UpdateMode.ALLOW) {
								// Check if read data is larger than previously read data.
								// If it's smaller or equals it means that RocksDB is overwriting the beginning of the result buffer.
								assert resultNioBuf.limit() > assertionReadData;
								if (ASSERTIONS_ENABLED) {
									assertionReadData = resultNioBuf.limit();
								}
							}

							// Check if read data is not bigger than the total value size.
							// If it's bigger it means that RocksDB is writing the start of the result into the result
							// buffer more than once.
							assert resultNioBuf.limit() <= valueSize;

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
								// Expand the resultBuf size if the result is bigger than the current result buffer size
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
			} finally {
				keyDirectBuf.release();
			}
		} finally {
			key.release();
		}
	}

	private void dbPut(ColumnFamilyHandle cfh, @Nullable WriteOptions writeOptions, ByteBuf key, ByteBuf value)
			throws RocksDBException {
		if (!key.isDirect()) {
			throw new RocksDBException("Key buffer must be direct");
		}
		if (!value.isDirect()) {
			throw new RocksDBException("Value buffer must be direct");
		}
		try {
			ByteBuf keyDirectBuffer = key.retain();
			var keyNioBuffer = LLUtils.toDirectFast(keyDirectBuffer.retain());
			if (keyNioBuffer == null) {
				keyDirectBuffer.release();
				keyDirectBuffer = LLUtils.toDirectCopy(key.retain());
				keyNioBuffer = keyDirectBuffer.nioBuffer();
			}
			try {
				assert keyNioBuffer.isDirect();


				ByteBuf valueDirectBuffer = value.retain();
				var valueNioBuffer = LLUtils.toDirectFast(valueDirectBuffer.retain());
				if (valueNioBuffer == null) {
					valueDirectBuffer.release();
					valueDirectBuffer = LLUtils.toDirectCopy(value.retain());
					valueNioBuffer = valueDirectBuffer.nioBuffer();
				}
				try {
					assert valueNioBuffer.isDirect();
					db.put(cfh, Objects.requireNonNullElse(writeOptions, EMPTY_WRITE_OPTIONS), keyNioBuffer, valueNioBuffer);
				} finally {
					valueDirectBuffer.release();
				}
			} finally {
				keyDirectBuffer.release();
			}
		} finally {
			key.release();
			value.release();
		}
	}

	@Override
	public Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, LLRange range) {
		Mono<Boolean> contains;
		if (range.isSingle()) {
			contains = containsKey(snapshot, range.getSingle().retain());
		} else {
			contains = containsRange(snapshot, range.retain());
		}
		return contains.map(isContained -> !isContained).doFinally(s -> range.release());
	}

	public Mono<Boolean> containsRange(@Nullable LLSnapshot snapshot, LLRange range) {
		return Mono
				.fromCallable(() -> {
					var readOpts = resolveSnapshot(snapshot);
					readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
					readOpts.setFillCache(false);
					if (range.hasMin()) {
						readOpts.setIterateLowerBound(new DirectSlice(Objects.requireNonNull(LLUtils.toDirectFast(range.getMin().retain()),
								"This range must use direct buffers"
						)));
					}
					if (range.hasMax()) {
						readOpts.setIterateUpperBound(new DirectSlice(Objects.requireNonNull(LLUtils.toDirectFast(range.getMax().retain()),
								"This range must use direct buffers"
						)));
					}
					try (RocksIterator rocksIterator = db.newIterator(cfh, readOpts)) {
						if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
							rocksIterator.seek(Objects.requireNonNull(LLUtils.toDirectFast(range.getMin().retain()),
									"This range must use direct buffers"
							));
						} else {
							rocksIterator.seekToFirst();
						}
						return rocksIterator.isValid();
					}
				})
				.onErrorMap(cause -> new IOException("Failed to read range " + range.toString(), cause))
				.subscribeOn(dbScheduler)
				.doFinally(s -> range.release());
	}

	private Mono<Boolean> containsKey(@Nullable LLSnapshot snapshot, ByteBuf key) {
		return Mono
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
						int size = RocksDB.NOT_FOUND;
						byte[] keyBytes = LLUtils.toArray(key);
						Holder<byte[]> data = new Holder<>();
						if (db.keyMayExist(cfh, resolveSnapshot(snapshot), keyBytes, data)) {
							if (data.getValue() != null) {
								size = data.getValue().length;
							} else {
								size = db.get(cfh, resolveSnapshot(snapshot), keyBytes, NO_DATA);
							}
						}
						return size != RocksDB.NOT_FOUND;
					} finally {
						if (updateMode == UpdateMode.ALLOW) {
							lock.unlockRead(stamp);
						}
					}
				})
				.onErrorMap(cause -> new IOException("Failed to read " + LLUtils.toString(key), cause))
				.subscribeOn(dbScheduler)
				.doFinally(s -> key.release());
	}

	@Override
	public Mono<ByteBuf> put(ByteBuf key, ByteBuf value, LLDictionaryResultType resultType) {
		if (!key.isDirect()) {
			return Mono.fromCallable(() -> {
				throw new IllegalArgumentException("Key must not be direct");
			});
		}
		if (!value.isDirect()) {
			return Mono.fromCallable(() -> {
				throw new IllegalArgumentException("Value must not be direct");
			});
		}
		return getPreviousData(key.retain(), resultType)
				.concatWith(Mono
						.fromCallable(() -> {
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
									logger.trace("Writing {}: {}", LLUtils.toString(key), LLUtils.toString(value));
								}
								if (!key.isDirect()) {
									throw new IllegalArgumentException("Key must not be direct");
								}
								if (!value.isDirect()) {
									throw new IllegalArgumentException("Value must not be direct");
								}
								dbPut(cfh, null, key.retain(), value.retain());
								assert value.refCnt() > 0;
								return null;
							} finally {
								if (updateMode == UpdateMode.ALLOW) {
									lock.unlockWrite(stamp);
								}
							}
						})
						.onErrorMap(cause -> new IOException("Failed to write " + LLUtils.toString(key), cause))
						.subscribeOn(dbScheduler)
						.then(Mono.empty())
				)
				.singleOrEmpty()
				.doFinally(s -> {
					key.release();
					value.release();
				});
	}

	@Override
	public Mono<Boolean> update(ByteBuf key,
			Function<@Nullable ByteBuf, @Nullable ByteBuf> updater,
			boolean existsAlmostCertainly) {
		return Mono
				.fromCallable(() -> {
					if (updateMode == UpdateMode.DISALLOW) throw new UnsupportedOperationException("update() is disallowed");
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
							logger.trace("Reading {}", LLUtils.toString(key));
						}
						while (true) {
							boolean changed = false;
							@Nullable ByteBuf prevData;
							var prevDataHolder = existsAlmostCertainly ? null : new Holder<byte[]>();
							if (existsAlmostCertainly || db.keyMayExist(cfh, LLUtils.toArray(key), prevDataHolder)) {
								if (!existsAlmostCertainly && prevDataHolder.getValue() != null) {
									byte @Nullable [] prevDataBytes = prevDataHolder.getValue();
									if (prevDataBytes != null) {
										prevData = wrappedBuffer(prevDataBytes);
									} else {
										prevData = null;
									}
								} else {
									prevData = dbGet(cfh, null, key.retain());
								}
							} else {
								prevData = null;
							}
							try {
								@Nullable ByteBuf newData;
								ByteBuf prevDataToSendToUpdater = prevData == null ? null : prevData.retainedSlice();
								try {
									newData = updater.apply(
											prevDataToSendToUpdater == null ? null : prevDataToSendToUpdater.retain());
									assert prevDataToSendToUpdater == null || prevDataToSendToUpdater.readerIndex() == 0
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
											logger.trace("Deleting {}", LLUtils.toString(key));
										}
										changed = true;
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
											logger.trace("Writing {}: {}", LLUtils.toString(key), LLUtils.toString(newData));
										}
										changed = true;
										dbPut(cfh, null, key.retain(), newData.retain());
									}
									return changed;
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
				})
				.onErrorMap(cause -> new IOException("Failed to read or write " + LLUtils.toString(key), cause))
				.subscribeOn(dbScheduler)
				.doFinally(s -> key.release());
	}

	private void dbDelete(ColumnFamilyHandle cfh, @Nullable WriteOptions writeOptions, ByteBuf key)
			throws RocksDBException {
		try {
			if (!key.isDirect()) {
				throw new IllegalArgumentException("Key must be a direct buffer");
			}
			ByteBuf keyDirectBuffer = key.retain();
			var keyNioBuffer = LLUtils.toDirectFast(keyDirectBuffer.retain());
			if (keyNioBuffer == null) {
				keyDirectBuffer.release();
				keyDirectBuffer = LLUtils.toDirectCopy(key.retain());
				keyNioBuffer = keyDirectBuffer.nioBuffer();
			}
			try {
				db.delete(cfh, Objects.requireNonNullElse(writeOptions, EMPTY_WRITE_OPTIONS), keyNioBuffer);
			} finally {
				keyDirectBuffer.release();
			}
		} finally {
			key.release();
		}
	}

	@Override
	public Mono<ByteBuf> remove(ByteBuf key, LLDictionaryResultType resultType) {
		return getPreviousData(key.retain(), resultType)
				.concatWith(Mono
						.fromCallable(() -> {
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
									logger.trace("Deleting {}", LLUtils.toString(key));
								}
								dbDelete(cfh, null, key.retain());
								return null;
							} finally {
								if (updateMode == UpdateMode.ALLOW) {
									lock.unlockWrite(stamp);
								}
							}
						})
						.onErrorMap(cause -> new IOException("Failed to delete " + LLUtils.toString(key), cause))
						.subscribeOn(dbScheduler)
						.then(Mono.empty())
				).singleOrEmpty()
				.doFinally(s -> key.release());
	}

	private Mono<ByteBuf> getPreviousData(ByteBuf key, LLDictionaryResultType resultType) {
		Mono<ByteBuf> prevValue;
		switch (resultType) {
			case PREVIOUS_VALUE_EXISTENCE:
				prevValue = this
						.containsKey(null, key.retain())
						.single()
						.map(LLUtils::booleanToResponseByteBuffer)
						.doFinally(s -> {
							assert key.refCnt() > 0;
						});
				break;
			case PREVIOUS_VALUE:
				prevValue = Mono
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
											return dbGet(cfh, null, key.retain());
										} finally {
											assert key.refCnt() > 0;
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
						.onErrorMap(cause -> new IOException("Failed to read " + LLUtils.toString(key), cause))
						.subscribeOn(dbScheduler);
				break;
			case VOID:
				prevValue = Mono.empty();
				break;
			default:
				prevValue = Mono.error(new IllegalStateException("Unexpected value: " + resultType));
				break;
		}
		return prevValue.doFinally(s -> key.release());
	}

	@Override
	public Flux<Entry<ByteBuf, ByteBuf>> getMulti(@Nullable LLSnapshot snapshot,
			Flux<ByteBuf> keys,
			boolean existsAlmostCertainly) {
		return keys
				.window(MULTI_GET_WINDOW)
				.flatMap(keysWindowFlux -> keysWindowFlux.collectList()
						.flatMapMany(keysWindow -> Mono
								.fromCallable(() -> {
									Iterable<StampedLock> locks;
									ArrayList<Long> stamps;
									if (updateMode == UpdateMode.ALLOW) {
										locks = itemsLock.bulkGetAt(getLockIndices(keysWindow));
										stamps = new ArrayList<>();
										for (var lock : locks) {
											
											stamps.add(lock.readLock());
										}
									} else {
										locks = null;
										stamps = null;
									}
									try {
										var handlesArray = new ColumnFamilyHandle[keysWindow.size()];
										Arrays.fill(handlesArray, cfh);
										var handles = ObjectArrayList.wrap(handlesArray, handlesArray.length);
										var results = db.multiGetAsList(resolveSnapshot(snapshot), handles, LLUtils.toArray(keysWindow));
										var mappedResults = new ArrayList<Entry<ByteBuf, ByteBuf>>(results.size());
										for (int i = 0; i < results.size(); i++) {
											var val = results.get(i);
											if (val != null) {
												results.set(i, null);
												mappedResults.add(Map.entry(keysWindow.get(i).retain(), wrappedBuffer(val)));
											}
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
								.flatMapMany(Flux::fromIterable)
								.onErrorMap(cause -> new IOException("Failed to read keys "
										+ Arrays.deepToString(keysWindow.toArray(ByteBuf[]::new)), cause))
								.doFinally(s -> keysWindow.forEach(ReferenceCounted::release))
						)
				);
	}

	@Override
	public Flux<Entry<ByteBuf, ByteBuf>> putMulti(Flux<Entry<ByteBuf, ByteBuf>> entries, boolean getOldValues) {
		return entries
				.window(Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.flatMap(Flux::collectList)
				.flatMap(entriesWindow -> {
					Flux<Entry<ByteBuf, ByteBuf>> oldValues;
					if (getOldValues) {
						oldValues = this
								.getMulti(null, Flux
										.fromIterable(entriesWindow)
										.map(Entry::getKey)
										.map(ByteBuf::retain), false)
								.publishOn(dbScheduler);
					} else {
						oldValues = Flux.empty();
					}
					return oldValues
									.concatWith(Mono.fromCallable(() -> {
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
									}))
									.doFinally(s -> entriesWindow.forEach(entry -> {
										entry.getKey().release();
										entry.getValue().release();
									}));
						}
				);
	}


	@NotNull
	private Mono<Entry<ByteBuf, ByteBuf>> putEntryToWriteBatch(Entry<ByteBuf, ByteBuf> newEntry,
			boolean getOldValues, CappedWriteBatch writeBatch) {
		Mono<ByteBuf> getOldValueMono;
		if (getOldValues) {
			getOldValueMono = get(null, newEntry.getKey().retain(), false);
		} else {
			getOldValueMono = Mono.empty();
		}
		return getOldValueMono
				.concatWith(Mono
						.<ByteBuf>fromCallable(() -> {
							writeBatch.put(cfh, newEntry.getKey().retain(), newEntry.getValue().retain());
							return null;
						})
						.subscribeOn(dbScheduler)
				)
				.singleOrEmpty()
				.map(oldValue -> Map.entry(newEntry.getKey().retain(), oldValue))
				.doFinally(s -> {
					newEntry.getKey().release();
					newEntry.getValue().release();
				});
	}

	@Override
	public Flux<Entry<ByteBuf, ByteBuf>> getRange(@Nullable LLSnapshot snapshot,
			LLRange range,
			boolean existsAlmostCertainly) {
		Flux<Entry<ByteBuf, ByteBuf>> result;
		if (range.isSingle()) {
			result = getRangeSingle(snapshot, range.getMin().retain(), existsAlmostCertainly);
		} else {
			result = getRangeMulti(snapshot, range.retain());
		}
		return result.doFinally(s -> range.release());
	}

	@Override
	public Flux<List<Entry<ByteBuf, ByteBuf>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength, boolean existsAlmostCertainly) {
		Flux<List<Entry<ByteBuf, ByteBuf>>> result;
		if (range.isSingle()) {
			result = getRangeSingle(snapshot, range.getMin().retain(), existsAlmostCertainly).map(List::of);
		} else {
			result = getRangeMultiGrouped(snapshot, range.retain(), prefixLength);
		}
		return result.doFinally(s -> range.release());
	}

	private Flux<Entry<ByteBuf, ByteBuf>> getRangeSingle(LLSnapshot snapshot, ByteBuf key, boolean existsAlmostCertainly) {
		return this
				.get(snapshot, key.retain(), existsAlmostCertainly)
				.map(value -> Map.entry(key.retain(), value))
				.flux()
				.doFinally(s -> key.release());
	}

	private Flux<Entry<ByteBuf, ByteBuf>> getRangeMulti(LLSnapshot snapshot, LLRange range) {
		return new LLLocalEntryReactiveRocksIterator(db, alloc, cfh, range.retain(), resolveSnapshot(snapshot))
				.flux()
				.subscribeOn(dbScheduler)
				.doFinally(s -> range.release());
	}

	private Flux<List<Entry<ByteBuf, ByteBuf>>> getRangeMultiGrouped(LLSnapshot snapshot, LLRange range, int prefixLength) {
		return new LLLocalGroupedEntryReactiveRocksIterator(db,
				alloc,
				cfh,
				prefixLength,
				range.retain(),
				resolveSnapshot(snapshot),
				"getRangeMultiGrouped"
		)
				.flux()
				.subscribeOn(dbScheduler)
				.doFinally(s -> range.release());
	}

	@Override
	public Flux<ByteBuf> getRangeKeys(@Nullable LLSnapshot snapshot, LLRange range) {
		Flux<ByteBuf> result;
		if (range.isSingle()) {
			result = getRangeKeysSingle(snapshot, range.getMin().retain());
		} else {
			result = getRangeKeysMulti(snapshot, range.retain());
		}
		return result.doFinally(s -> range.release());
	}

	@Override
	public Flux<List<ByteBuf>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot, LLRange range, int prefixLength) {
		return new LLLocalGroupedKeyReactiveRocksIterator(db,
				alloc,
				cfh,
				prefixLength,
				range.retain(),
				resolveSnapshot(snapshot),
				"getRangeKeysGrouped"
		).flux().subscribeOn(dbScheduler).doFinally(s -> range.release());
	}

	@Override
	public Flux<ByteBuf> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, LLRange range, int prefixLength) {
		return new LLLocalKeyPrefixReactiveRocksIterator(db,
				alloc,
				cfh,
				prefixLength,
				range.retain(),
				resolveSnapshot(snapshot),
				true,
				"getRangeKeysGrouped"
		).flux().subscribeOn(dbScheduler).doFinally(s -> range.release());
	}

	private Flux<ByteBuf> getRangeKeysSingle(LLSnapshot snapshot, ByteBuf key) {
		return this
				.containsKey(snapshot, key.retain())
				.filter(contains -> contains)
				.map(contains -> key.retain())
				.flux()
				.doFinally(s -> key.release());
	}

	private Flux<ByteBuf> getRangeKeysMulti(LLSnapshot snapshot, LLRange range) {
		return new LLLocalKeyReactiveRocksIterator(db, alloc, cfh, range.retain(), resolveSnapshot(snapshot))
				.flux()
				.subscribeOn(dbScheduler)
				.doFinally(s -> range.release());
	}

	@Override
	public Flux<Entry<ByteBuf, ByteBuf>> setRange(LLRange range,
			Flux<Entry<ByteBuf, ByteBuf>> entries,
			boolean getOldValues) {
		Flux<Entry<ByteBuf, ByteBuf>> oldValues;
		if (getOldValues) {
			oldValues = getRange(null, range);
		} else {
			oldValues = Flux.empty();
		}
		return oldValues
				.concatWith(Flux
						.usingWhen(
								Mono
										.fromCallable(() -> new CappedWriteBatch(db,
												CAPPED_WRITE_BATCH_CAP,
												RESERVED_WRITE_BATCH_SIZE,
												MAX_WRITE_BATCH_SIZE,
												BATCH_WRITE_OPTIONS)
										)
										.subscribeOn(dbScheduler),
								writeBatch -> Mono
										.fromCallable(() -> {
											if (range.isSingle()) {
												writeBatch.delete(cfh, LLUtils.toArray(range.getSingle().retain()));
											} else {
												deleteSmallRangeWriteBatch(writeBatch, range.retain());
											}
											return null;
										})
										.subscribeOn(dbScheduler)
										.thenMany(entries)
										.<Entry<ByteBuf, ByteBuf>>concatMap(newEntry -> this
												.putEntryToWriteBatch(newEntry, false, writeBatch)
												.then(Mono.empty())
										),
								writeBatch -> Mono
										.fromCallable(() -> {
											try (writeBatch) {
												writeBatch.writeToDbAndClose();
											}
											return null;
										})
										.subscribeOn(dbScheduler)
						)
						.subscribeOn(dbScheduler)
						.onErrorMap(cause -> new IOException("Failed to write range", cause))
						.doFinally(s -> range.release())
				);
	}

	private void deleteSmallRangeWriteBatch(CappedWriteBatch writeBatch, LLRange range)
			throws RocksDBException {
		var readOpts = getReadOptions(null);
		readOpts.setFillCache(false);
		ReleasableSlice minBound;
		if (range.hasMin()) {
			minBound = setIterateBound(readOpts, IterateBound.LOWER, range.getMin().retain());
		} else {
			minBound = EMPTY_RELEASABLE_SLICE;
		}
		ReleasableSlice maxBound;
		if (range.hasMax()) {
			maxBound = setIterateBound(readOpts, IterateBound.UPPER, range.getMax().retain());
		} else {
			maxBound = EMPTY_RELEASABLE_SLICE;
		}
		try (var rocksIterator = db.newIterator(cfh, readOpts)) {
			if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
				rocksIterSeekTo(rocksIterator, range.getMin().retain());
			} else {
				rocksIterator.seekToFirst();
			}
			while (rocksIterator.isValid()) {
				writeBatch.delete(cfh, LLUtils.readDirectNioBuffer(alloc, rocksIterator::key));
				rocksIterator.next();
			}
		} finally {
			minBound.release();
			maxBound.release();
			range.release();
		}
	}

	private static void rocksIterSeekTo(RocksIterator rocksIterator, ByteBuf buffer) {
		try {
			ByteBuf directBuffer = buffer.retain();
			ByteBuffer nioBuffer = LLUtils.toDirectFast(directBuffer.retain());
			if (nioBuffer == null) {
				directBuffer.release();
				directBuffer = LLUtils.toDirectCopy(buffer.retain());
				assert directBuffer.isDirect();
				nioBuffer = directBuffer.nioBuffer();
			}
			try {
				assert nioBuffer.isDirect();
				rocksIterator.seek(nioBuffer);
			} finally {
				directBuffer.release();
			}
		} finally {
			buffer.release();
		}
	}

	private static ReleasableSlice setIterateBound(ReadOptions readOpts, IterateBound boundType, ByteBuf buffer) {
		try {
			ByteBuf directBuffer = buffer.retain();
			ByteBuffer nioBuffer = LLUtils.toDirectFast(directBuffer.retain());
			if (nioBuffer == null) {
				directBuffer = LLUtils.toDirectCopy(buffer.retain());
				assert directBuffer.isDirect();
				nioBuffer = directBuffer.nioBuffer();
			}
			AbstractSlice<?> slice;
			try {
				assert nioBuffer.isDirect();
				slice = new DirectSlice(nioBuffer);
				if (boundType == IterateBound.LOWER) {
					readOpts.setIterateLowerBound(slice);
				} else {
					readOpts.setIterateUpperBound(slice);
				}
			} catch (Throwable t) {
				directBuffer.release();
				throw t;
			}
			return new ReleasableSlice(slice, directBuffer);
		} finally {
			buffer.release();
		}
	}

	private static final ReleasableSlice EMPTY_RELEASABLE_SLICE = new ReleasableSlice(new Slice(new byte[0]), null) {
		@Override
		public void release() {
		}
	};

	@Data
	@AllArgsConstructor
	public static class ReleasableSlice {
		AbstractSlice<?> slice;
		@Nullable ByteBuf byteBuf;

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
					var readOpts = getReadOptions(null);
					readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);

					// readOpts.setIgnoreRangeDeletions(true);
					readOpts.setFillCache(false);
					//readOpts.setReadaheadSize(2 * 1024 * 1024);
					try (CappedWriteBatch writeBatch = new CappedWriteBatch(db,
							CAPPED_WRITE_BATCH_CAP,
							RESERVED_WRITE_BATCH_SIZE,
							MAX_WRITE_BATCH_SIZE,
							BATCH_WRITE_OPTIONS
					)) {

						byte[] firstDeletedKey = null;
						byte[] lastDeletedKey = null;
						try (RocksIterator iter = db.newIterator(cfh, readOpts)) {
							iter.seekToLast();

							if (iter.isValid()) {
								firstDeletedKey = FIRST_KEY;
								lastDeletedKey = iter.key();
								writeBatch.deleteRange(cfh, FIRST_KEY, iter.key());
								writeBatch.delete(cfh, iter.key());
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
				})
				.onErrorMap(cause -> new IOException("Failed to clear", cause))
				.subscribeOn(dbScheduler);

	}

	@Override
	public Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, LLRange range, boolean fast) {
		Mono<Long> result;
		if (range.isAll()) {
			result = Mono
					.fromCallable(() -> fast ? fastSizeAll(snapshot) : exactSizeAll(snapshot))
					.onErrorMap(IOException::new)
					.subscribeOn(dbScheduler);
		} else {
			result = Mono
					.fromCallable(() -> {
						var readOpts = resolveSnapshot(snapshot);
						readOpts.setFillCache(false);
						readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
						ReleasableSlice minBound;
						if (range.hasMin()) {
							minBound = setIterateBound(readOpts, IterateBound.LOWER, range.getMin().retain());
						} else {
							minBound = EMPTY_RELEASABLE_SLICE;
						}
						ReleasableSlice maxBound;
						if (range.hasMax()) {
							maxBound = setIterateBound(readOpts, IterateBound.UPPER, range.getMax().retain());
						} else {
							maxBound = EMPTY_RELEASABLE_SLICE;
						}
						try {
							if (fast) {
								readOpts.setIgnoreRangeDeletions(true);

							}
							try (var rocksIterator = db.newIterator(cfh, readOpts)) {
								if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
									rocksIterSeekTo(rocksIterator, range.getMin().retain());
								} else {
									rocksIterator.seekToFirst();
								}
								long i = 0;
								while (rocksIterator.isValid()) {
									rocksIterator.next();
									i++;
								}
								return i;
							}
						} finally {
							minBound.release();
							maxBound.release();
						}
					})
					.onErrorMap(cause -> new IOException("Failed to get size of range "
							+ range.toString(), cause))
					.subscribeOn(dbScheduler);
		}
		return result.doFinally(s -> range.release());
	}

	@Override
	public Mono<Entry<ByteBuf, ByteBuf>> getOne(@Nullable LLSnapshot snapshot, LLRange range) {
		return Mono
				.fromCallable(() -> {
					var readOpts = resolveSnapshot(snapshot);
					ReleasableSlice minBound;
					if (range.hasMin()) {
						minBound = setIterateBound(readOpts, IterateBound.LOWER, range.getMin().retain());
					} else {
						minBound = EMPTY_RELEASABLE_SLICE;
					}
					ReleasableSlice maxBound;
					if (range.hasMax()) {
						maxBound = setIterateBound(readOpts, IterateBound.UPPER, range.getMax().retain());
					} else {
						maxBound = EMPTY_RELEASABLE_SLICE;
					}
					try (var rocksIterator = db.newIterator(cfh, readOpts)) {
						if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
							rocksIterSeekTo(rocksIterator, range.getMin().retain());
						} else {
							rocksIterator.seekToFirst();
						}
						ByteBuf key;
						if (rocksIterator.isValid()) {
							key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
							return Map.entry(key, LLUtils.readDirectNioBuffer(alloc, rocksIterator::value));
						} else {
							return null;
						}
					} finally {
						minBound.release();
						maxBound.release();
					}
				})
				.subscribeOn(dbScheduler)
				.doFinally(s -> range.release());
	}

	@Override
	public Mono<ByteBuf> getOneKey(@Nullable LLSnapshot snapshot, LLRange range) {
		return Mono
				.fromCallable(() -> {
					var readOpts = resolveSnapshot(snapshot);
					ReleasableSlice minBound;
					if (range.hasMin()) {
						minBound = setIterateBound(readOpts, IterateBound.LOWER, range.getMin().retain());
					} else {
						minBound = EMPTY_RELEASABLE_SLICE;
					}
					ReleasableSlice maxBound;
					if (range.hasMax()) {
						maxBound = setIterateBound(readOpts, IterateBound.UPPER, range.getMax().retain());
					} else {
						maxBound = EMPTY_RELEASABLE_SLICE;
					}
					try (var rocksIterator = db.newIterator(cfh, readOpts)) {
						if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
							rocksIterSeekTo(rocksIterator, range.getMin().retain());
						} else {
							rocksIterator.seekToFirst();
						}
						ByteBuf key;
						if (rocksIterator.isValid()) {
							key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
							return key;
						} else {
							return null;
						}
					} finally {
						minBound.release();
						maxBound.release();
					}
				})
				.subscribeOn(dbScheduler)
				.doFinally(s -> range.release());
	}

	private long fastSizeAll(@Nullable LLSnapshot snapshot) {
		var rocksdbSnapshot = resolveSnapshot(snapshot);
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
			try (RocksIterator iter = db.newIterator(cfh, rocksdbSnapshot)) {
				iter.seekToFirst();
				// If it's a fast size of a snapshot, count only up to 100'000 elements
				while (iter.isValid() && count < 100_000) {
					count++;
					iter.next();
				}
				return count;
			}
		}
	}

	private long exactSizeAll(@Nullable LLSnapshot snapshot) {
		var readOpts = resolveSnapshot(snapshot);
		readOpts.setFillCache(false);
		//readOpts.setReadaheadSize(2 * 1024 * 1024);
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
						var rangeReadOpts = new ReadOptions(readOpts);
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
							try (RocksIterator iter = db.newIterator(cfh, rangeReadOpts)) {
								iter.seekToFirst();
								while (iter.isValid()) {
									partialCount++;
									iter.next();
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

	@Override
	public Mono<Entry<ByteBuf, ByteBuf>> removeOne(LLRange range) {
		return Mono
				.fromCallable(() -> {
					var readOpts = getReadOptions(null);
					ReleasableSlice minBound;
					if (range.hasMin()) {
						minBound = setIterateBound(readOpts, IterateBound.LOWER, range.getMin().retain());
					} else {
						minBound = EMPTY_RELEASABLE_SLICE;
					}
					ReleasableSlice maxBound;
					if (range.hasMax()) {
						maxBound = setIterateBound(readOpts, IterateBound.UPPER, range.getMax().retain());
					} else {
						maxBound = EMPTY_RELEASABLE_SLICE;
					}
					try (RocksIterator rocksIterator = db.newIterator(cfh, readOpts)) {
						if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
							rocksIterSeekTo(rocksIterator, range.getMin().retain());
						} else {
							rocksIterator.seekToFirst();
						}
						if (!rocksIterator.isValid()) {
							return null;
						}
						ByteBuf key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
						ByteBuf value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value);
						dbDelete(cfh, null, key);
						return Map.entry(key, value);
					} finally {
						minBound.release();
						maxBound.release();
					}
				})
				.onErrorMap(cause -> new IOException("Failed to delete " + range.toString(), cause))
				.subscribeOn(dbScheduler)
				.doFinally(s -> range.release());
	}

	@NotNull
	public static Tuple3<RocksIterator, ReleasableSlice, ReleasableSlice> getRocksIterator(ReadOptions readOptions,
			LLRange range,
			RocksDB db,
			ColumnFamilyHandle cfh) {
		try {
			ReleasableSlice sliceMin;
			ReleasableSlice sliceMax;
			if (range.hasMin()) {
				sliceMin = setIterateBound(readOptions, IterateBound.LOWER, range.getMin().retain());
			} else {
				sliceMin = EMPTY_RELEASABLE_SLICE;
			}
			if (range.hasMax()) {
				sliceMax = setIterateBound(readOptions, IterateBound.UPPER, range.getMax().retain());
			} else {
				sliceMax = EMPTY_RELEASABLE_SLICE;
			}
			var rocksIterator = db.newIterator(cfh, readOptions);
			if (!PREFER_SEEK_TO_FIRST && range.hasMin()) {
				rocksIterSeekTo(rocksIterator, range.getMin().retain());
			} else {
				rocksIterator.seekToFirst();
			}
			return Tuples.of(rocksIterator, sliceMin, sliceMax);
		} finally {
			range.release();
		}
	}
}
