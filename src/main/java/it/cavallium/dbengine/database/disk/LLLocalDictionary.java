package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;
import org.warp.commonutils.type.VariableWrapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

@NotAtomic
public class LLLocalDictionary implements LLDictionary {

	private static final boolean USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS = true;
	static final int RESERVED_WRITE_BATCH_SIZE = 2 * 1024 * 1024; // 2MiB
	static final long MAX_WRITE_BATCH_SIZE = 1024L * 1024L * 1024L; // 1GiB
	static final int CAPPED_WRITE_BATCH_CAP = 50000; // 50K operations
	static final int MULTI_GET_WINDOW = 500;
	static final WriteOptions BATCH_WRITE_OPTIONS = new WriteOptions().setLowPri(true);

	private static final byte[] FIRST_KEY = new byte[]{};
	private static final byte[] NO_DATA = new byte[0];
	private static final ReadOptions EMPTY_READ_OPTIONS = new ReadOptions();
	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final String databaseName;
	private final Scheduler dbScheduler;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;

	public LLLocalDictionary(@NotNull RocksDB db,
			@NotNull ColumnFamilyHandle columnFamilyHandle,
			String databaseName,
			Scheduler dbScheduler,
			Function<LLSnapshot, Snapshot> snapshotResolver) {
		Objects.requireNonNull(db);
		this.db = db;
		Objects.requireNonNull(columnFamilyHandle);
		this.cfh = columnFamilyHandle;
		this.databaseName = databaseName;
		this.dbScheduler = dbScheduler;
		this.snapshotResolver = snapshotResolver;
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

	@Override
	public Mono<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key) {
		return Mono
				.fromCallable(() -> {
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
				})
				.onErrorMap(IOException::new)
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
					try (RocksIterator iter = db.newIterator(cfh, resolveSnapshot(snapshot))) {
						if (range.hasMin()) {
							iter.seek(range.getMin());
						} else {
							iter.seekToFirst();
						}
						if (!iter.isValid()) {
							return false;
						}

						if (range.hasMax()) {
							byte[] key1 = iter.key();
							return Arrays.compareUnsigned(key1, range.getMax()) <= 0;
						} else {
							return true;
						}
					}
				})
				.onErrorMap(IOException::new)
				.subscribeOn(dbScheduler);
	}

	private Mono<Boolean> containsKey(@Nullable LLSnapshot snapshot, byte[] key) {
		return Mono
				.fromCallable(() -> {
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
				})
				.onErrorMap(IOException::new)
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<byte[]> put(byte[] key, byte[] value, LLDictionaryResultType resultType) {
		Mono<byte[]> response = getPrevValue(key, resultType);
		return Mono
				.fromCallable(() -> {
					db.put(cfh, key, value);
					return null;
				})
				.onErrorMap(IOException::new)
				.subscribeOn(dbScheduler)
				.then(response);
	}

	@Override
	public Mono<byte[]> remove(byte[] key, LLDictionaryResultType resultType) {
		Mono<byte[]> response = getPrevValue(key, resultType);
		return Mono
				.fromCallable(() -> {
					db.delete(cfh, key);
					return null;
				})
				.onErrorMap(IOException::new)
				.subscribeOn(dbScheduler)
				.then(response);
	}

	private Mono<byte[]> getPrevValue(byte[] key, LLDictionaryResultType resultType) {
		switch (resultType) {
			case VALUE_CHANGED:
				return containsKey(null, key).single().map(LLUtils::booleanToResponse);
			case PREVIOUS_VALUE:
				return Mono
						.fromCallable(() -> {
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
						})
						.onErrorMap(IOException::new)
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
								.<ArrayList<Entry<byte[], byte[]>>>fromCallable(() -> {
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
								})
								.subscribeOn(dbScheduler)
								.<Entry<byte[], byte[]>>flatMapMany(Flux::fromIterable)
						)
				)
				.onErrorMap(IOException::new);
	}

	@Override
	public Flux<Entry<byte[], byte[]>> putMulti(Flux<Entry<byte[], byte[]>> entries, boolean getOldValues) {
		return entries
				.window(Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.publishOn(dbScheduler)
				.flatMap(Flux::collectList)
				.flatMap(entriesWindow -> this
						.getMulti(null, Flux.fromIterable(entriesWindow).map(Entry::getKey))
						.concatWith(Mono.fromCallable(() -> {
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
						})));
	}


	@NotNull
	private Mono<Entry<byte[], byte[]>> putEntryToWriteBatch(Entry<byte[], byte[]> newEntry, boolean getOldValues,
			CappedWriteBatch writeBatch) {
		return Mono.from(Mono
				.defer(() -> {
					if (getOldValues) {
						return get(null, newEntry.getKey());
					} else {
						return Mono.empty();
					}
				})
				.concatWith(Mono.<byte[]>fromCallable(() -> {
					synchronized (writeBatch) {
						writeBatch.put(cfh, newEntry.getKey(), newEntry.getValue());
					}
					return null;
				})
						.subscribeOn(dbScheduler))
				.map(oldValue -> Map.entry(newEntry.getKey(), oldValue)));
	}

	@NotNull
	private Flux<Entry<byte[], byte[]>> putEntryToWriteBatch(List<Entry<byte[], byte[]>> newEntries, boolean getOldValues,
			CappedWriteBatch writeBatch) {
		return Flux
				.from(Flux
						.defer(() -> {
							if (getOldValues) {
								return getMulti(null, Flux.fromIterable(newEntries).map(Entry::getKey));
							} else {
								return Flux.empty();
							}
						})
						.concatWith(Mono
								.<Entry<byte[], byte[]>>fromCallable(() -> {
									synchronized (writeBatch) {
										for (Entry<byte[], byte[]> newEntry : newEntries) {
											writeBatch.put(cfh, newEntry.getKey(), newEntry.getValue());
										}
									}
									return null;
								}).subscribeOn(dbScheduler)
						)
				);
	}

	@Override
	public Flux<Entry<byte[], byte[]>> getRange(@Nullable LLSnapshot snapshot, LLRange range) {
		if (range.isSingle()) {
			return getRangeSingle(snapshot, range.getMin());
		} else {
			return getRangeMulti(snapshot, range);
		}
	}

	private Flux<Entry<byte[],byte[]>> getRangeSingle(LLSnapshot snapshot, byte[] key) {
		return this
				.get(snapshot, key)
				.map(value -> Map.entry(key, value))
				.flux();
	}

	private Flux<Entry<byte[],byte[]>> getRangeMulti(LLSnapshot snapshot, LLRange range) {
		return Mono
				.fromCallable(() -> {
					var iter = db.newIterator(cfh, resolveSnapshot(snapshot));
					if (range.hasMin()) {
						iter.seek(range.getMin());
					} else {
						iter.seekToFirst();
					}
					return iter;
				})
				.subscribeOn(dbScheduler)
				.flatMapMany(rocksIterator -> Flux
						.<Entry<byte[], byte[]>>fromIterable(() -> {
							VariableWrapper<byte[]> nextKey = new VariableWrapper<>(null);
							VariableWrapper<byte[]> nextValue = new VariableWrapper<>(null);
							return new Iterator<>() {
								@Override
								public boolean hasNext() {
									assert nextKey.var == null;
									assert nextValue.var == null;
									if (!rocksIterator.isValid()) {
										nextKey.var = null;
										nextValue.var = null;
										return false;
									}
									var key = rocksIterator.key();
									var value = rocksIterator.value();
									if (range.hasMax() && Arrays.compareUnsigned(key, range.getMax()) > 0) {
										nextKey.var = null;
										nextValue.var = null;
										return false;
									}
									nextKey.var = key;
									nextValue.var = value;
									return true;
								}

								@Override
								public Entry<byte[], byte[]> next() {
									var key = nextKey.var;
									var val = nextValue.var;
									assert key != null;
									assert val != null;
									nextKey.var = null;
									nextValue.var = null;
									return Map.entry(key, val);
								}
							};
						})
						.doFinally(signalType -> rocksIterator.close())
						.subscribeOn(dbScheduler)
				);
	}

	@Override
	public Flux<byte[]> getRangeKeys(@Nullable LLSnapshot snapshot, LLRange range) {
		if (range.isSingle()) {
			return getRangeKeysSingle(snapshot, range.getMin());
		} else {
			return getRangeKeysMulti(snapshot, range);
		}
	}

	private Flux<byte[]> getRangeKeysSingle(LLSnapshot snapshot, byte[] key) {
		return this
				.containsKey(snapshot, key)
				.filter(contains -> contains)
				.map(contains -> key)
				.flux();
	}

	private Flux<byte[]> getRangeKeysMulti(LLSnapshot snapshot, LLRange range) {
		return Mono
				.fromCallable(() -> {
					var iter = db.newIterator(cfh, resolveSnapshot(snapshot));
					if (range.hasMin()) {
						iter.seek(range.getMin());
					} else {
						iter.seekToFirst();
					}
					return iter;
				})
				.subscribeOn(dbScheduler)
				.flatMapMany(rocksIterator -> Flux
						.<byte[]>fromIterable(() -> {
							VariableWrapper<byte[]> nextKey = new VariableWrapper<>(null);
							return new Iterator<>() {
								@Override
								public boolean hasNext() {
									assert nextKey.var == null;
									if (!rocksIterator.isValid()) {
										nextKey.var = null;
										return false;
									}
									var key = rocksIterator.key();
									var value = rocksIterator.value();
									if (range.hasMax() && Arrays.compareUnsigned(key, range.getMax()) > 0) {
										nextKey.var = null;
										return false;
									}
									nextKey.var = key;
									return true;
								}

								@Override
								public byte[] next() {
									var key = nextKey.var;
									assert key != null;
									nextKey.var = null;
									return key;
								}
							};
						})
						.doFinally(signalType -> rocksIterator.close())
						.subscribeOn(dbScheduler)
				);
	}

	@Override
	public Flux<Entry<byte[], byte[]>> setRange(LLRange range,
			Flux<Entry<byte[], byte[]>> entries,
			boolean getOldValues) {
		if (range.isAll()) {
			return clear().thenMany(Flux.empty());
		} else {
			return Mono
					.fromCallable(() -> new CappedWriteBatch(db, CAPPED_WRITE_BATCH_CAP, RESERVED_WRITE_BATCH_SIZE, MAX_WRITE_BATCH_SIZE, BATCH_WRITE_OPTIONS))
					.subscribeOn(dbScheduler)
					.flatMapMany(writeBatch -> Mono
							.fromCallable(() -> {
								synchronized (writeBatch) {
									if (range.hasMin() && range.hasMax()) {
										writeBatch.deleteRange(cfh, range.getMin(), range.getMax());
										writeBatch.delete(cfh, range.getMax());
									} else if (range.hasMax()) {
										writeBatch.deleteRange(cfh, FIRST_KEY, range.getMax());
										writeBatch.delete(cfh, range.getMax());
									} else {
										try (var it = db.newIterator(cfh, getReadOptions(null))) {
											it.seekToLast();
											if (it.isValid()) {
												writeBatch.deleteRange(cfh, range.getMin(), it.key());
												writeBatch.delete(cfh, it.key());
											}
										}
									}
								}
								return null;
							})
							.subscribeOn(dbScheduler)
							.thenMany(entries)
							.flatMap(newEntry -> putEntryToWriteBatch(newEntry, getOldValues, writeBatch))
							.concatWith(Mono
									.<Entry<byte[], byte[]>>fromCallable(() -> {
										synchronized (writeBatch) {
											writeBatch.writeToDbAndClose();
											writeBatch.close();
										}
										return null;
									})
									.subscribeOn(dbScheduler)
							)
							.doFinally(signalType -> {
								synchronized (writeBatch) {
									writeBatch.close();
								}
							})
					)
					.onErrorMap(IOException::new);
		}
	}

	public Mono<Void> clear() {
		return Mono
				.<Void>fromCallable(() -> {
					try (RocksIterator iter = db.newIterator(cfh); CappedWriteBatch writeBatch = new CappedWriteBatch(db,
							CAPPED_WRITE_BATCH_CAP,
							RESERVED_WRITE_BATCH_SIZE,
							MAX_WRITE_BATCH_SIZE,
							BATCH_WRITE_OPTIONS
					)) {

						iter.seekToFirst();

						while (iter.isValid()) {
							writeBatch.delete(cfh, iter.key());

							iter.next();
						}

						writeBatch.writeToDbAndClose();

						// Compact range
						db.compactRange(cfh);

						db.flush(new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true), cfh);
						db.flushWal(true);
					}
					return null;
				})
				.onErrorMap(IOException::new)
				.subscribeOn(dbScheduler);

	}

	@Override
	public Mono<Long> sizeRange(@Nullable LLSnapshot snapshot, LLRange range, boolean fast) {
		return Mono
				.defer(() -> {
					if (range.isAll()) {
						return Mono
								.fromCallable(() -> fast ? fastSizeAll(snapshot) : exactSizeAll(snapshot))
								.onErrorMap(IOException::new)
								.subscribeOn(dbScheduler);
					} else {
						return Mono
								.fromCallable(() -> {
									try (var iter = db.newIterator(cfh, resolveSnapshot(snapshot))) {
										if (range.hasMin()) {
											iter.seek(range.getMin());
										} else {
											iter.seekToFirst();
										}
										long i = 0;
										while (iter.isValid()) {
											if (range.hasMax()) {
												byte[] key1 = iter.key();
												if (Arrays.compareUnsigned(key1, range.getMax()) > 0) {
													break;
												}
											}

											iter.next();
											i++;
										}
										return i;
									}
								})
								.onErrorMap(IOException::new)
								.subscribeOn(dbScheduler);
					}
				});
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
			long count = 0;
			try (RocksIterator iter = db.newIterator(cfh, rocksdbSnapshot)) {
				iter.seekToFirst();
				// If it's a fast size of a snapshot, count only up to 1000 elements
				while (iter.isValid() && count < 1000) {
					count++;
					iter.next();
				}
				return count;
			}
		}
	}

	private long exactSizeAll(@Nullable LLSnapshot snapshot) {
		long count = 0;
		try (RocksIterator iter = db.newIterator(cfh, resolveSnapshot(snapshot))) {
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
					try (RocksIterator iter = db.newIterator(cfh)) {
						if (range.hasMin()) {
							iter.seek(range.getMin());
						} else {
							iter.seekToFirst();
						}
						if (!iter.isValid()) {
							return null;
						}
						if (range.hasMax() && Arrays.compareUnsigned(iter.key(), range.getMax()) > 0) {
							return null;
						}
						byte[] key = iter.key();
						byte[] value = iter.value();
						db.delete(cfh, key);
						return Map.entry(key, value);
					}
				})
				.onErrorMap(IOException::new)
				.subscribeOn(dbScheduler);
	}
}
