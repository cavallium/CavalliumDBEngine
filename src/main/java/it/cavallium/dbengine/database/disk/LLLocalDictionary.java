package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
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

@NotAtomic
public class LLLocalDictionary implements LLDictionary {

	protected static final Logger logger = LoggerFactory.getLogger(LLLocalDictionary.class);
	private static final boolean USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS = true;
	static final int RESERVED_WRITE_BATCH_SIZE = 2 * 1024 * 1024; // 2MiB
	static final long MAX_WRITE_BATCH_SIZE = 1024L * 1024L * 1024L; // 1GiB
	static final int CAPPED_WRITE_BATCH_CAP = 50000; // 50K operations
	static final int MULTI_GET_WINDOW = 500;
	static final WriteOptions BATCH_WRITE_OPTIONS = new WriteOptions().setLowPri(true);

	private static final int STRIPES = 512;
	private static final byte[] FIRST_KEY = new byte[]{};
	private static final byte[] NO_DATA = new byte[0];
	private static final ReadOptions EMPTY_READ_OPTIONS = new ReadOptions();
	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final String databaseName;
	private final Scheduler dbScheduler;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;
	private final Striped<StampedLock> itemsLock = Striped.readWriteStampedLock(STRIPES);
	private final UpdateMode updateMode;

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

	private int getLockIndex(byte[] key) {
		return Math.abs(Arrays.hashCode(key) % STRIPES);
	}

	private IntArrayList getLockIndices(List<byte[]> keys) {
		var list = new IntArrayList(keys.size());
		for (byte[] key : keys) {
			list.add(getLockIndex(key));
		}
		return list;
	}

	private IntArrayList getLockIndicesEntries(List<Entry<byte[], byte[]>> keys) {
		var list = new IntArrayList(keys.size());
		for (Entry<byte[], byte[]> key : keys) {
			list.add(getLockIndex(key.getKey()));
		}
		return list;
	}

	@Override
	public Mono<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key) {
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
						logger.trace("Reading {}", key);
						Holder<byte[]> data = new Holder<>();
						if (db.keyMayExist(cfh, resolveSnapshot(snapshot), key, data)) {
							if (data.getValue() != null) {
								return data.getValue();
							} else {
								return db.get(cfh, resolveSnapshot(snapshot), key);
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
				.onErrorMap(cause -> new IOException("Failed to read " + Arrays.toString(key), cause))
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<Boolean> isRangeEmpty(@Nullable LLSnapshot snapshot, LLRange range) {
		if (range.isSingle()) {
			return containsKey(snapshot, range.getSingle()).map(contains -> !contains);
		} else {
			return containsRange(snapshot, range).map(contains -> !contains);
		}
	}

	public Mono<Boolean> containsRange(@Nullable LLSnapshot snapshot, LLRange range) {
		return Mono
				.fromCallable(() -> {
					var readOpts = resolveSnapshot(snapshot);
					readOpts.setVerifyChecksums(false);
					readOpts.setFillCache(false);
					if (range.hasMin()) {
						readOpts.setIterateLowerBound(new Slice(range.getMin()));
					}
					if (range.hasMax()) {
						readOpts.setIterateUpperBound(new Slice(range.getMax()));
					}
					try (RocksIterator iter = db.newIterator(cfh, readOpts)) {
						iter.seekToFirst();
						return iter.isValid();
					}
				})
				.onErrorMap(cause -> new IOException("Failed to read range " + range.toString(), cause))
				.subscribeOn(dbScheduler);
	}

	private Mono<Boolean> containsKey(@Nullable LLSnapshot snapshot, byte[] key) {
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
						Holder<byte[]> data = new Holder<>();
						if (db.keyMayExist(cfh, resolveSnapshot(snapshot), key, data)) {
							if (data.getValue() != null) {
								size = data.getValue().length;
							} else {
								size = db.get(cfh, resolveSnapshot(snapshot), key, NO_DATA);
							}
						}
						return size != RocksDB.NOT_FOUND;
					} finally {
						if (updateMode == UpdateMode.ALLOW) {
							lock.unlockRead(stamp);
						}
					}
				})
				.onErrorMap(cause -> new IOException("Failed to read " + Arrays.toString(key), cause))
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<byte[]> put(byte[] key, byte[] value, LLDictionaryResultType resultType) {
		return getPrevValue(key, resultType)
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
								logger.trace("Writing {}: {}", key, value);
								db.put(cfh, key, value);
								return null;
							} finally {
								if (updateMode == UpdateMode.ALLOW) {
									lock.unlockWrite(stamp);
								}
							}
						})
						.onErrorMap(cause -> new IOException("Failed to write " + Arrays.toString(key), cause))
						.subscribeOn(dbScheduler)
						.then(Mono.empty())
				).singleOrEmpty();
	}

	@Override
	public Mono<Boolean> update(byte[] key, Function<Optional<byte[]>, Optional<byte[]>> value) {
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
								logger.trace("Reading {}", key);
								while (true) {
									boolean changed = false;
									Optional<byte[]> prevData;
									var prevDataHolder = new Holder<byte[]>();
									if (db.keyMayExist(cfh, key, prevDataHolder)) {
										if (prevDataHolder.getValue() != null) {
											prevData = Optional.ofNullable(prevDataHolder.getValue());
										} else {
											prevData = Optional.ofNullable(db.get(cfh, key));
										}
									} else {
										prevData = Optional.empty();
									}

									Optional<byte[]> newData = value.apply(prevData);
									if (prevData.isPresent() && newData.isEmpty()) {
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
										logger.trace("Deleting {}", key);
										changed = true;
										db.delete(cfh, key);
									} else if (newData.isPresent()
											&& (prevData.isEmpty() || !Arrays.equals(prevData.get(), newData.get()))) {
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
										logger.trace("Writing {}: {}", key, newData.get());
										changed = true;
										db.put(cfh, key, newData.get());
									}
									return changed;
								}
							} finally {
								if (updateMode == UpdateMode.ALLOW) {
									lock.unlock(stamp);
								}
							}
						})
						.onErrorMap(cause -> new IOException("Failed to read or write " + Arrays.toString(key), cause))
						.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<byte[]> remove(byte[] key, LLDictionaryResultType resultType) {
		return getPrevValue(key, resultType)
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
								db.delete(cfh, key);
								return null;
							} finally {
								if (updateMode == UpdateMode.ALLOW) {
									lock.unlockWrite(stamp);
								}
							}
						})
						.onErrorMap(cause -> new IOException("Failed to delete " + Arrays.toString(key), cause))
						.subscribeOn(dbScheduler)
						.then(Mono.empty())
				).singleOrEmpty();
	}

	private Mono<byte[]> getPrevValue(byte[] key, LLDictionaryResultType resultType) {
		switch (resultType) {
			case VALUE_CHANGED:
				return containsKey(null, key).single().map(LLUtils::booleanToResponse);
			case PREVIOUS_VALUE:
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
								logger.trace("Reading {}", key);
								var data = new Holder<byte[]>();
								if (db.keyMayExist(cfh, key, data)) {
									if (data.getValue() != null) {
										return data.getValue();
									} else {
										return db.get(cfh, key);
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
						.onErrorMap(cause -> new IOException("Failed to read " + Arrays.toString(key), cause))
						.subscribeOn(dbScheduler);
			case VOID:
				return Mono.empty();
			default:
				return Mono.error(new IllegalStateException("Unexpected value: " + resultType));
		}
	}

	@Override
	public Flux<Entry<byte[], byte[]>> getMulti(@Nullable LLSnapshot snapshot, Flux<byte[]> keys) {
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
										var results = db.multiGetAsList(resolveSnapshot(snapshot), handles, keysWindow);
										var mappedResults = new ArrayList<Entry<byte[], byte[]>>(results.size());
										for (int i = 0; i < results.size(); i++) {
											var val = results.get(i);
											if (val != null) {
												results.set(i, null);
												mappedResults.add(Map.entry(keysWindow.get(i), val));
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
										+ Arrays.deepToString(keysWindow.toArray(byte[][]::new)), cause))
						)
				);
	}

	@Override
	public Flux<Entry<byte[], byte[]>> putMulti(Flux<Entry<byte[], byte[]>> entries, boolean getOldValues) {
		return entries
				.window(Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.flatMap(Flux::collectList)
				.flatMap(entriesWindow -> this
						.getMulti(null, Flux.fromIterable(entriesWindow).map(Entry::getKey))
						.publishOn(dbScheduler)
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
								for (Entry<byte[], byte[]> entry : entriesWindow) {
									batch.put(entry.getKey(), entry.getValue());
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
						})));
	}


	@NotNull
	private Mono<Entry<byte[], byte[]>> putEntryToWriteBatch(Entry<byte[], byte[]> newEntry, boolean getOldValues,
			CappedWriteBatch writeBatch) {
		Mono<byte[]> getOldValueMono;
		if (getOldValues) {
			getOldValueMono = get(null, newEntry.getKey());
		} else {
			getOldValueMono = Mono.empty();
		}
		return getOldValueMono
				.concatWith(Mono
						.<byte[]>fromCallable(() -> {
							writeBatch.put(cfh, newEntry.getKey(), newEntry.getValue());
							return null;
						})
						.subscribeOn(dbScheduler)
				)
				.singleOrEmpty()
				.map(oldValue -> Map.entry(newEntry.getKey(), oldValue));
	}

	@Override
	public Flux<Entry<byte[], byte[]>> getRange(@Nullable LLSnapshot snapshot, LLRange range) {
		if (range.isSingle()) {
			return getRangeSingle(snapshot, range.getMin());
		} else {
			return getRangeMulti(snapshot, range);
		}
	}

	@Override
	public Flux<List<Entry<byte[], byte[]>>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength) {
		if (range.isSingle()) {
			return getRangeSingle(snapshot, range.getMin()).map(List::of);
		} else {
			return getRangeMultiGrouped(snapshot, range, prefixLength);
		}
	}

	private Flux<Entry<byte[],byte[]>> getRangeSingle(LLSnapshot snapshot, byte[] key) {
		return this
				.get(snapshot, key)
				.map(value -> Map.entry(key, value))
				.flux();
	}

	private Flux<Entry<byte[],byte[]>> getRangeMulti(LLSnapshot snapshot, LLRange range) {
		return new LLLocalLuceneEntryReactiveIterator(db, cfh, range, resolveSnapshot(snapshot))
				.flux()
				.subscribeOn(dbScheduler);
	}

	private Flux<List<Entry<byte[],byte[]>>> getRangeMultiGrouped(LLSnapshot snapshot, LLRange range, int prefixLength) {
		return new LLLocalLuceneGroupedEntryReactiveIterator(db,
				cfh,
				prefixLength,
				range,
				resolveSnapshot(snapshot),
				"getRangeMultiGrouped"
		)
				.flux()
				.subscribeOn(dbScheduler);
	}

	@Override
	public Flux<byte[]> getRangeKeys(@Nullable LLSnapshot snapshot, LLRange range) {
		if (range.isSingle()) {
			return getRangeKeysSingle(snapshot, range.getMin());
		} else {
			return getRangeKeysMulti(snapshot, range);
		}
	}

	@Override
	public Flux<List<byte[]>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot, LLRange range, int prefixLength) {
		return new LLLocalLuceneGroupedKeysReactiveIterator(db,
				cfh,
				prefixLength,
				range,
				resolveSnapshot(snapshot),
				"getRangeKeysGrouped"
		).flux().subscribeOn(dbScheduler);
	}

	@Override
	public Flux<byte[]> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, LLRange range, int prefixLength) {
		return new LLLocalLuceneKeyPrefixesReactiveIterator(db,
				cfh,
				prefixLength,
				range,
				resolveSnapshot(snapshot),
				"getRangeKeysGrouped"
		).flux().subscribeOn(dbScheduler);
	}

	private Flux<byte[]> getRangeKeysSingle(LLSnapshot snapshot, byte[] key) {
		return this
				.containsKey(snapshot, key)
				.filter(contains -> contains)
				.map(contains -> key)
				.flux();
	}

	private Flux<byte[]> getRangeKeysMulti(LLSnapshot snapshot, LLRange range) {
		return new LLLocalLuceneKeysReactiveIterator(db, cfh, range, resolveSnapshot(snapshot)).flux().subscribeOn(dbScheduler);
	}

	@Override
	public Flux<Entry<byte[], byte[]>> setRange(LLRange range,
			Flux<Entry<byte[], byte[]>> entries,
			boolean getOldValues) {
		return Flux
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
										writeBatch.delete(cfh, range.getSingle());
									} else if (range.hasMin() && range.hasMax()) {
										writeBatch.deleteRange(cfh, range.getMin(), range.getMax());
									} else if (range.hasMax()) {
										writeBatch.deleteRange(cfh, FIRST_KEY, range.getMax());
									} else if (range.hasMin()) {
										// Delete from x to end of column
										var readOpts = getReadOptions(null);
										readOpts.setIterateLowerBound(new Slice(range.getMin()));
										try (var it = db.newIterator(cfh, readOpts)) {
											it.seekToLast();
											if (it.isValid()) {
												writeBatch.deleteRange(cfh, range.getMin(), it.key());
												// Delete the last key because we are deleting everything from "min" onward, without a max bound
												writeBatch.delete(it.key());
											}
										}
									} else {
										// Delete all
										var readOpts = getReadOptions(null);
										try (var it = db.newIterator(cfh, readOpts)) {
											it.seekToLast();
											if (it.isValid()) {
												writeBatch.deleteRange(cfh, FIRST_KEY, it.key());
												// Delete the last key because we are deleting everything without a max bound
												writeBatch.delete(it.key());
											}
										}
									}
									return null;
								})
								.subscribeOn(dbScheduler)
								.thenMany(entries)
								.flatMapSequential(newEntry -> putEntryToWriteBatch(newEntry, getOldValues, writeBatch)),
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
				.onErrorMap(cause -> new IOException("Failed to write range", cause));
	}

	private static byte[] incrementLexicographically(byte[] key) {
		boolean remainder = true;
		int prefixLength = key.length;
		final byte ff = (byte) 0xFF;
		for (int i = prefixLength - 1; i >= 0; i--) {
			if (key[i] != ff) {
				key[i]++;
				remainder = false;
				break;
			} else {
				key[i] = 0x00;
				remainder = true;
			}
		}

		if (remainder) {
			Arrays.fill(key, 0, prefixLength, (byte) 0xFF);
			return Arrays.copyOf(key, key.length + 1);
		} else {
			return key;
		}
	}

	public Mono<Void> clear() {
		return Mono
				.<Void>fromCallable(() -> {
					var readOpts = getReadOptions(null);
					readOpts.setVerifyChecksums(false);

					// readOpts.setIgnoreRangeDeletions(true);
					readOpts.setFillCache(false);
					try (CappedWriteBatch writeBatch = new CappedWriteBatch(db,
							CAPPED_WRITE_BATCH_CAP,
							RESERVED_WRITE_BATCH_SIZE,
							MAX_WRITE_BATCH_SIZE,
							BATCH_WRITE_OPTIONS
					)) {

						//byte[] firstDeletedKey = null;
						//byte[] lastDeletedKey = null;
						try (RocksIterator iter = db.newIterator(cfh, readOpts)) {
							iter.seekToLast();

							if (iter.isValid()) {
								writeBatch.deleteRange(cfh, FIRST_KEY, iter.key());
								writeBatch.delete(cfh, iter.key());
								//firstDeletedKey = FIRST_KEY;
								//lastDeletedKey = incrementLexicographically(iter.key());
							}
						}

						writeBatch.writeToDbAndClose();

						// Compact range
						db.suggestCompactRange(cfh);
						//if (firstDeletedKey != null && lastDeletedKey != null) {
							//db.compactRange(cfh, firstDeletedKey, lastDeletedKey, new CompactRangeOptions().setChangeLevel(false));
						//}

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
		if (range.isAll()) {
			return Mono
					.fromCallable(() -> fast ? fastSizeAll(snapshot) : exactSizeAll(snapshot))
					.onErrorMap(IOException::new)
					.subscribeOn(dbScheduler);
		} else {
			return Mono
					.fromCallable(() -> {
						var readOpts = resolveSnapshot(snapshot);
						readOpts.setFillCache(false);
						readOpts.setVerifyChecksums(false);
						if (range.hasMin()) {
							readOpts.setIterateLowerBound(new Slice(range.getMin()));
						}
						if (range.hasMax()) {
							readOpts.setIterateUpperBound(new Slice(range.getMax()));
						}
						if (fast) {
							readOpts.setIgnoreRangeDeletions(true);

						}
						try (var iter = db.newIterator(cfh, readOpts)) {
							iter.seekToFirst();
							long i = 0;
							while (iter.isValid()) {
								iter.next();
								i++;
							}
							return i;
						}
					})
					.onErrorMap(cause -> new IOException("Failed to get size of range "
							+ range.toString(), cause))
					.subscribeOn(dbScheduler);
		}
	}

	@Override
	public Mono<Entry<byte[], byte[]>> getOne(@Nullable LLSnapshot snapshot, LLRange range) {
		return Mono
				.fromCallable(() -> {
					var readOpts = resolveSnapshot(snapshot);
					if (range.hasMin()) {
						readOpts.setIterateLowerBound(new Slice(range.getMin()));
					}
					if (range.hasMax()) {
						readOpts.setIterateUpperBound(new Slice(range.getMax()));
					}
					try (var rocksIterator = db.newIterator(cfh, readOpts)) {
						rocksIterator.seekToFirst();
						byte[] key;
						if (rocksIterator.isValid()) {
							key = rocksIterator.key();
							return Map.entry(key, rocksIterator.value());
						} else {
							return null;
						}
					}
				})
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<byte[]> getOneKey(@Nullable LLSnapshot snapshot, LLRange range) {
		return Mono
				.fromCallable(() -> {
					var readOpts = resolveSnapshot(snapshot);
					if (range.hasMin()) {
						readOpts.setIterateLowerBound(new Slice(range.getMin()));
					}
					if (range.hasMax()) {
						readOpts.setIterateUpperBound(new Slice(range.getMax()));
					}
					try (var rocksIterator = db.newIterator(cfh, readOpts)) {
						rocksIterator.seekToFirst();
						byte[] key;
						if (rocksIterator.isValid()) {
							key = rocksIterator.key();
							return key;
						} else {
							return null;
						}
					}
				})
				.subscribeOn(dbScheduler);
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
		} else {
			rocksdbSnapshot.setFillCache(false);
			rocksdbSnapshot.setVerifyChecksums(false);
			rocksdbSnapshot.setPinData(false);
			rocksdbSnapshot.setIgnoreRangeDeletions(false);
			if (snapshot == null) {
				rocksdbSnapshot.setTailing(true);
			}
			long count = 0;
			try (RocksIterator iter = db.newIterator(cfh, rocksdbSnapshot)) {
				iter.seekToFirst();
				// If it's a fast size of a snapshot, count only up to 1'000'000 elements
				while (iter.isValid() && count < 1_000_000) {
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
		readOpts.setVerifyChecksums(false);

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

	@Override
	public Mono<Entry<byte[], byte[]>> removeOne(LLRange range) {
		return Mono
				.fromCallable(() -> {
					var readOpts = getReadOptions(null);
					if (range.hasMin()) {
						readOpts.setIterateLowerBound(new Slice(range.getMin()));
					}
					if (range.hasMax()) {
						readOpts.setIterateUpperBound(new Slice(range.getMax()));
					}
					try (RocksIterator iter = db.newIterator(cfh, readOpts)) {
						if (range.hasMin()) {
							iter.seek(range.getMin());
						} else {
							iter.seekToFirst();
						}
						if (!iter.isValid()) {
							return null;
						}
						byte[] key = iter.key();
						byte[] value = iter.value();
						db.delete(cfh, key);
						return Map.entry(key, value);
					}
				})
				.onErrorMap(cause -> new IOException("Failed to delete " + range.toString(), cause))
				.subscribeOn(dbScheduler);
	}
}
