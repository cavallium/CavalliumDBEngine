package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.isBoundedRange;
import static it.cavallium.dbengine.database.LLUtils.mapList;
import static it.cavallium.dbengine.database.LLUtils.toStringSafe;
import static it.cavallium.dbengine.database.LLUtils.wrapNullable;
import static it.cavallium.dbengine.database.disk.UpdateAtomicResultMode.DELTA;
import static it.cavallium.dbengine.utils.StreamUtils.ROCKSDB_POOL;
import static it.cavallium.dbengine.utils.StreamUtils.collectOn;
import static it.cavallium.dbengine.utils.StreamUtils.executing;
import static it.cavallium.dbengine.utils.StreamUtils.fastSummingLong;
import static it.cavallium.dbengine.utils.StreamUtils.streamWhileNonNull;
import static java.util.Objects.requireNonNull;
import static it.cavallium.dbengine.utils.StreamUtils.batches;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.database.ColumnUtils;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.OptionalBuf;
import it.cavallium.dbengine.database.SerializedKey;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.disk.rocksdb.LLReadOptions;
import it.cavallium.dbengine.database.disk.rocksdb.LLWriteOptions;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.utils.DBException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;

public class LLLocalDictionary implements LLDictionary {

	protected static final Logger logger = LogManager.getLogger(LLLocalDictionary.class);
	private static final boolean USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS = false;
	static final int RESERVED_WRITE_BATCH_SIZE = 2 * 1024 * 1024; // 2MiB
	static final long MAX_WRITE_BATCH_SIZE = 1024L * 1024L * 1024L; // 1GiB
	static final int CAPPED_WRITE_BATCH_CAP = 50000; // 50K operations
	static final int MULTI_GET_WINDOW = 16;
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
	private static final boolean USE_NUM_ENTRIES_PRECISE_COUNTER = true;

	private static final byte[] FIRST_KEY = new byte[]{};

	private final RocksDBColumn db;
	private final ColumnFamilyHandle cfh;
	private final String databaseName;
	private final String columnName;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;
	private final UpdateMode updateMode;

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
			@NotNull RocksDBColumn db,
			String databaseName,
			String columnName,
			Function<LLSnapshot, Snapshot> snapshotResolver,
			UpdateMode updateMode,
			DatabaseOptions databaseOptions) {
		requireNonNull(db);
		this.db = db;
		this.cfh = db.getColumnFamilyHandle();
		this.databaseName = databaseName;
		this.columnName = columnName;
		this.snapshotResolver = snapshotResolver;
		this.updateMode = updateMode;
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
	private LLReadOptions generateReadOptionsOrNew(LLSnapshot snapshot) {
		return generateReadOptions(snapshot != null ? snapshotResolver.apply(snapshot) : null);
	}

	private LLReadOptions generateReadOptions(Snapshot snapshot) {
		if (snapshot != null) {
			return new LLReadOptions().setSnapshot(snapshot);
		} else {
			return new LLReadOptions();
		}
	}

	@Override
	public Buf get(@Nullable LLSnapshot snapshot, Buf key) {
		return this.getSync(snapshot, key);
	}

	private Buf getSync(LLSnapshot snapshot, Buf key) {
		logger.trace(MARKER_ROCKSDB, "Reading {}", () -> toStringSafe(key));
		try {
			Buf result;
			startedGet.increment();
			try (var readOptions = generateReadOptionsOrNew(snapshot)) {
				var initTime = System.nanoTime();
				result = db.get(readOptions, key);
				getTime.record(Duration.ofNanos(System.nanoTime() - initTime));
			} finally {
				endedGet.increment();
			}
			logger.trace(MARKER_ROCKSDB, "Read {}: {}", () -> toStringSafe(key), () -> toStringSafe(result));
			return result;
		} catch (RocksDBException ex) {
			throw new DBException("Failed to read " + toStringSafe(key) + ": " + ex.getMessage());
		}
	}

	@Override
	public boolean isRangeEmpty(@Nullable LLSnapshot snapshot, LLRange range, boolean fillCache) {
		assert !LLUtils.isInNonBlockingThread() : "Called isRangeEmpty in a nonblocking thread";
		startedContains.increment();
		try {
			Boolean isRangeEmpty = containsTime.recordCallable(() -> {
				if (range.isSingle()) {
					return !containsKey(snapshot, range.getSingleUnsafe());
				} else {
					// Temporary resources to release after finished

					try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNew(snapshot),
							true,
							isBoundedRange(range),
							true
					)) {
						readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
						readOpts.setFillCache(fillCache);
						try (var rocksIterator = db.newIterator(readOpts, range.getMin(), range.getMax())) {
							if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
								var seekArray = LLUtils.asArray(range.getMin());
								rocksIterator.seek(seekArray);
							} else {
								rocksIterator.seekToFirst();
							}
							return !rocksIterator.isValid();
						}
					}
				}
			});
			assert isRangeEmpty != null;
			return isRangeEmpty;
		} catch (Exception ex) {
			throw new DBException("Failed to read range " + LLUtils.toStringSafe(range), ex);
		} finally {
			endedContains.increment();
		}
	}

	private boolean containsKey(@Nullable LLSnapshot snapshot, Buf key) throws RocksDBException {
		startedContains.increment();
		try {
			var result = containsTime.recordCallable(() -> {
				try (var readOptions = generateReadOptionsOrNew(snapshot)) {
					return db.exists(readOptions, key);
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
	public Buf put(Buf key, Buf value, LLDictionaryResultType resultType) {
		// Obtain the previous value from the database
		var previousData = this.getPreviousData(key, resultType);
		putInternal(key, value);
		return previousData;
	}

	private void putInternal(Buf key, Buf value) {
		if (logger.isTraceEnabled(MARKER_ROCKSDB)) {
			var varargs = new Supplier<?>[]{() -> toStringSafe(key), () -> toStringSafe(value)};
			logger.trace(MARKER_ROCKSDB, "Writing {}: {}", varargs);
		}
		startedPut.increment();
		try (var writeOptions = new LLWriteOptions()) {
			putTime.recordCallable(() -> {
				db.put(writeOptions, key, value);
				return null;
			});
		} catch (RocksDBException ex) {
			throw new DBException("Failed to write: " + ex.getMessage());
		} catch (RuntimeException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to write", ex);
		} finally {
			endedPut.increment();
		}
	}

	@Override
	public UpdateMode getUpdateMode() {
		return updateMode;
	}

	@SuppressWarnings("DuplicatedCode")
	@Override
	public Buf update(Buf key,
			SerializationFunction<@Nullable Buf, @Nullable Buf> updater,
			UpdateReturnMode updateReturnMode) {
		assert !LLUtils.isInNonBlockingThread() : "Called update in a nonblocking thread";
		if (updateMode == UpdateMode.DISALLOW) {
			throw new UnsupportedOperationException("update() is disallowed");
		}
		UpdateAtomicResultMode returnMode = switch (updateReturnMode) {
			case NOTHING -> UpdateAtomicResultMode.NOTHING;
			case GET_NEW_VALUE -> UpdateAtomicResultMode.CURRENT;
			case GET_OLD_VALUE -> UpdateAtomicResultMode.PREVIOUS;
		};
		UpdateAtomicResult result = null;
		try {
			startedUpdates.increment();
			try (var readOptions = generateReadOptionsOrNew(null);
					var writeOptions = new LLWriteOptions()) {
				result = updateTime.recordCallable(() -> db.updateAtomic(readOptions, writeOptions, key, updater, returnMode));
			} finally {
				endedUpdates.increment();
			}
			assert result != null;
			return switch (updateReturnMode) {
				case NOTHING -> {
					yield null;
				}
				case GET_NEW_VALUE -> ((UpdateAtomicResultCurrent) result).current();
				case GET_OLD_VALUE -> ((UpdateAtomicResultPrevious) result).previous();
			};
		} catch (Exception ex) {
			throw new DBException("Failed to update key-value pair", ex);
		}
	}

	@SuppressWarnings("DuplicatedCode")
	@Override
	public LLDelta updateAndGetDelta(Buf key, SerializationFunction<@Nullable Buf, @Nullable Buf> updater) {
		assert !LLUtils.isInNonBlockingThread() : "Called update in a nonblocking thread";
		if (updateMode == UpdateMode.DISALLOW) {
			throw new UnsupportedOperationException("update() is disallowed");
		}
		if (updateMode == UpdateMode.ALLOW && !db.supportsTransactions()) {
			throw new UnsupportedOperationException("update() is disallowed because the database doesn't support"
					+ "safe atomic operations");
		}

		UpdateAtomicResultDelta result;
		try {
			startedUpdates.increment();
			try (var readOptions = generateReadOptionsOrNew(null);
					var writeOptions = new LLWriteOptions()) {
				result = updateTime.recordCallable(() -> (UpdateAtomicResultDelta) db.updateAtomic(readOptions,
						writeOptions,
						key,
						updater,
						DELTA
				));
			} finally {
				endedUpdates.increment();
			}
			assert result != null;
			return result.delta();
		} catch (Exception ex) {
			throw new DBException("Failed to update key-value pair and/or return the delta", ex);
		}
	}

	@Override
	public Buf remove(Buf key, LLDictionaryResultType resultType) {
		// Obtain the previous value from the database
		Buf previousData = this.getPreviousData(key, resultType);
		// Delete the value from the database
		try {
			logger.trace(MARKER_ROCKSDB, "Deleting {}", () -> toStringSafe(key));
			startedRemove.increment();
			try (var writeOptions = new LLWriteOptions()) {
				removeTime.recordCallable(() -> {
					db.delete(writeOptions, key);
					return null;
				});
			} finally {
				endedRemove.increment();
			}
			return previousData;
		} catch (Exception ex) {
			throw new DBException("Failed to delete", ex);
		}
	}

	private Buf getPreviousData(Buf key, LLDictionaryResultType resultType) {
		try {
			return switch (resultType) {
				case PREVIOUS_VALUE_EXISTENCE -> {
					var contained = containsKey(null, key);
					yield LLUtils.booleanToResponseByteBuffer(contained);
				}
				case PREVIOUS_VALUE -> {
					assert !LLUtils.isInNonBlockingThread() : "Called getPreviousData in a nonblocking thread";
					Buf result;
					try (var readOptions = generateReadOptionsOrNew(null)) {
						result = db.get(readOptions, key);
					}
					logger.trace(MARKER_ROCKSDB, "Read {}: {}", () -> toStringSafe(key), () -> toStringSafe(result));
					yield result;
				}
				case VOID -> null;
			};
		} catch (RocksDBException ex) {
			throw new DBException("Failed to read previous data");
		}
	}

	@Override
	public Stream<OptionalBuf> getMulti(@Nullable LLSnapshot snapshot, Stream<Buf> keys) {
		return keys.map(key -> OptionalBuf.ofNullable(getSync(snapshot, key)));
	}

	@Override
	public void putMulti(Stream<LLEntry> entries) {
		collectOn(ROCKSDB_POOL,
				batches(entries, Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP)),
				executing(entriesWindow -> {
					try (var writeOptions = new LLWriteOptions()) {
						assert !LLUtils.isInNonBlockingThread() : "Called putMulti in a nonblocking thread";
						if (USE_WRITE_BATCHES_IN_PUT_MULTI) {
							try (var batch = new CappedWriteBatch(db,
									CAPPED_WRITE_BATCH_CAP,
									RESERVED_WRITE_BATCH_SIZE,
									MAX_WRITE_BATCH_SIZE,
									writeOptions
							)) {
								for (LLEntry entry : entriesWindow) {
									batch.put(cfh, entry.getKey(), entry.getValue());
								}
								batch.flush();
							}
						} else {
							for (LLEntry entry : entriesWindow) {
								db.put(writeOptions, entry.getKey(), entry.getValue());
							}
						}
					} catch (RocksDBException ex) {
						throw new CompletionException(new DBException("Failed to write: " + ex.getMessage()));
					}
				})
		);
	}

	@Override
	public <K> Stream<Boolean> updateMulti(Stream<SerializedKey<K>> keys,
			KVSerializationFunction<K, @Nullable Buf, @Nullable Buf> updateFunction) {
		record MappedInput<K>(K key, Buf serializedKey, OptionalBuf mapped) {}
		return batches(keys, Math.min(MULTI_GET_WINDOW, CAPPED_WRITE_BATCH_CAP))
				.flatMap(entriesWindow -> {
					try (var writeOptions = new LLWriteOptions()) {
						if (LLUtils.isInNonBlockingThread()) {
							throw new UnsupportedOperationException("Called updateMulti in a nonblocking thread");
						}
						List<Buf> keyBufsWindow = new ArrayList<>(entriesWindow.size());
						for (var objects : entriesWindow) {
							keyBufsWindow.add(objects.serialized());
						}
						ArrayList<MappedInput<K>> mappedInputs;
						{
							try (var readOptions = generateReadOptionsOrNew(null)) {
								var inputs = db.multiGetAsList(readOptions, mapList(keyBufsWindow, Buf::asArray));
								mappedInputs = new ArrayList<>(inputs.size());
								for (int i = 0; i < inputs.size(); i++) {
									var val = inputs.get(i);
									if (val != null) {
										inputs.set(i, null);
										mappedInputs.add(new MappedInput<>(entriesWindow.get(i).key(),
												keyBufsWindow.get(i),
												OptionalBuf.of(Buf.wrap(val))
										));
									} else {
										mappedInputs.add(new MappedInput<>(entriesWindow.get(i).key(),
												keyBufsWindow.get(i),
												OptionalBuf.empty()
										));
									}
								}
							}
						}
						var updatedValuesToWrite = new ArrayList<Buf>(mappedInputs.size());
						var valueChangedResult = new ArrayList<Boolean>(mappedInputs.size());
						for (var mappedInput : mappedInputs) {
							var updatedValue = updateFunction.apply(mappedInput.key(), mappedInput.serializedKey());
							var t3 = mappedInput.mapped().orElse(null);
							valueChangedResult.add(!LLUtils.equals(t3, updatedValue));
							updatedValuesToWrite.add(updatedValue);
						}

						if (USE_WRITE_BATCHES_IN_PUT_MULTI) {
							try (var batch = new CappedWriteBatch(db,
									CAPPED_WRITE_BATCH_CAP,
									RESERVED_WRITE_BATCH_SIZE,
									MAX_WRITE_BATCH_SIZE,
									writeOptions
							)) {
								int i = 0;
								for (var entry : entriesWindow) {
									var valueToWrite = updatedValuesToWrite.get(i);
									if (valueToWrite == null) {
										batch.delete(cfh, entry.serialized());
									} else {
										batch.put(cfh, entry.serialized(), valueToWrite);
									}
									i++;
								}
								batch.flush();
							}
						} else {
							int i = 0;
							for (var entry : entriesWindow) {
								db.put(writeOptions, entry.serialized(), updatedValuesToWrite.get(i));
								i++;
							}
						}
						return valueChangedResult.stream();
					} catch (RocksDBException e) {
						throw new CompletionException(new DBException("Failed to update multiple key-value pairs", e));
					}
				});
	}

	@Override
	public Stream<LLEntry> getRange(@Nullable LLSnapshot snapshot,
			LLRange range,
			boolean reverse,
			boolean smallRange) {
		if (range.isSingle()) {
			var rangeSingle = range.getSingle();
			return getRangeSingle(snapshot, rangeSingle);
		} else {
			return getRangeMulti(snapshot, range, reverse, smallRange);
		}
	}

	@Override
	public Stream<List<LLEntry>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength,
			boolean smallRange) {
		if (range.isSingle()) {
			var rangeSingle = range.getSingle();

			return getRangeSingle(snapshot, rangeSingle).map(Collections::singletonList);
		} else {
			return getRangeMultiGrouped(snapshot, range, prefixLength, smallRange);
		}
	}

	private Stream<LLEntry> getRangeSingle(LLSnapshot snapshot, Buf key) {
		var val = this.get(snapshot, key);
		if (val == null) return Stream.of();
		return Stream.of(LLEntry.of(key, val));
	}

	private Stream<LLEntry> getRangeMulti(LLSnapshot snapshot,
			LLRange range,
			boolean reverse,
			boolean smallRange) {
		return new LLLocalEntryReactiveRocksIterator(db,
				range,
				() -> generateReadOptionsOrNew(snapshot),
				reverse,
				smallRange
		).stream();
	}

	private Stream<List<LLEntry>> getRangeMultiGrouped(LLSnapshot snapshot, LLRange range,
			int prefixLength, boolean smallRange) {
		return new LLLocalGroupedEntryReactiveRocksIterator(db,
				prefixLength,
				range,
				() -> generateReadOptionsOrNew(snapshot),
				smallRange
		).stream();
	}

	@Override
	public Stream<Buf> getRangeKeys(@Nullable LLSnapshot snapshot,
			LLRange range,
			boolean reverse,
			boolean smallRange) {
		if (range.isSingle()) {
			return this.getRangeKeysSingle(snapshot, range.getSingle());
		} else {
			return this.getRangeKeysMulti(snapshot, range, reverse, smallRange);
		}
	}

	@Override
	public Stream<List<Buf>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength,
			boolean smallRange) {
		return new LLLocalGroupedKeyReactiveRocksIterator(db,
				prefixLength,
				range,
				() -> generateReadOptionsOrNew(snapshot),
				smallRange
		).stream();
	}

	@Override
	public Stream<BadBlock> badBlocks(LLRange range) {
		try {
			var ro = LLUtils.generateCustomReadOptions(null,
					false,
					isBoundedRange(range),
					false
			);
			ro.setFillCache(false);
			if (!range.isSingle()) {
				if (LLUtils.MANUAL_READAHEAD) {
					ro.setReadaheadSize(32 * 1024);
				}
			}
			ro.setVerifyChecksums(true);
			var rocksIterator = db.newRocksIterator(ro, range, false);
			try {
				rocksIterator.seekToFirst();
			} catch (Exception ex) {
				rocksIterator.close();
				ro.close();
				throw new DBException("Failed to open rocksdb iterator", ex);
			}
			return streamWhileNonNull(() -> {
				if (!rocksIterator.isValid()) return null;
				Buf rawKey = null;
				try {
					rawKey = rocksIterator.keyBuf().copy();
					rocksIterator.next();
				} catch (RocksDBException ex) {
					return new BadBlock(databaseName, ColumnUtils.special(columnName), rawKey, ex);
				}
				return null;
			}).takeWhile(x -> rocksIterator.isValid()).onClose(() -> {
				rocksIterator.close();
				ro.close();
			});
		} catch (RocksDBException e) {
			throw new DBException("Failed to get bad blocks", e);
		}
	}

	@Override
	public Stream<Buf> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot, LLRange range,
			int prefixLength, boolean smallRange) {
		return new LLLocalKeyPrefixReactiveRocksIterator(db,
				prefixLength,
				range,
				() -> generateReadOptionsOrNew(snapshot),
				true,
				smallRange
		).stream();
	}

	private Stream<Buf> getRangeKeysSingle(LLSnapshot snapshot, Buf key) {
		try {
			if (containsKey(snapshot, key)) {
				return Stream.of(key);
			} else {
				return Stream.empty();
			}
		} catch (RocksDBException e) {
			throw new DBException("Failed to get range keys", e);
		}
	}

	private Stream<Buf> getRangeKeysMulti(LLSnapshot snapshot,
			LLRange range,
			boolean reverse,
			boolean smallRange) {
		return new LLLocalKeyReactiveRocksIterator(db,
				range,
				() -> generateReadOptionsOrNew(snapshot),
				reverse,
				smallRange
		).stream();
	}

	@Override
	public void setRange(LLRange range, Stream<LLEntry> entries, boolean smallRange) {
		if (USE_WINDOW_IN_SET_RANGE) {
			try (var writeOptions = new LLWriteOptions()) {
				assert !LLUtils.isInNonBlockingThread() : "Called setRange in a nonblocking thread";
				if (!USE_WRITE_BATCH_IN_SET_RANGE_DELETE || !USE_WRITE_BATCHES_IN_SET_RANGE) {
					try (var opts = LLUtils.generateCustomReadOptions(null, true, isBoundedRange(range), smallRange)) {
						try (var it = db.newIterator(opts, range.getMin(), range.getMax())) {
							if (!PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
								it.seekTo(range.getMin());
							} else {
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
							batch.delete(cfh, LLUtils.asArray(range.getSingleUnsafe()));
						} else {
							deleteSmallRangeWriteBatch(batch, range.copy());
						}
						db.write(writeOptions, batch);
						batch.clear();
					}
				}
			} catch (RocksDBException ex) {
				throw new DBException("Failed to set a range: " + ex.getMessage());
			}

			collectOn(ROCKSDB_POOL, batches(entries, MULTI_GET_WINDOW), executing(entriesList -> {
				try (var writeOptions = new LLWriteOptions()) {
					if (!USE_WRITE_BATCHES_IN_SET_RANGE) {
						for (LLEntry entry : entriesList) {
							db.put(writeOptions, entry.getKey(), entry.getValue());
						}
					} else if (USE_CAPPED_WRITE_BATCH_IN_SET_RANGE) {

						try (var batch = new CappedWriteBatch(db,
								CAPPED_WRITE_BATCH_CAP,
								RESERVED_WRITE_BATCH_SIZE,
								MAX_WRITE_BATCH_SIZE,
								writeOptions
						)) {
							for (LLEntry entry : entriesList) {
								batch.put(cfh, entry.getKey(), entry.getValue());
							}
							batch.flush();
						}
					} else {
						try (var batch = new WriteBatch(RESERVED_WRITE_BATCH_SIZE)) {
							for (LLEntry entry : entriesList) {
								batch.put(cfh, LLUtils.asArray(entry.getKey()), LLUtils.asArray(entry.getValue()));
							}
							db.write(writeOptions, batch);
							batch.clear();
						}
					}
				} catch (RocksDBException ex) {
					throw new CompletionException(new DBException("Failed to write range", ex));
				}
			}));
		} else {
			if (USE_WRITE_BATCHES_IN_SET_RANGE) {
				throw new UnsupportedOperationException("Can't use write batches in setRange without window. Please fix the parameters");
			}
			collectOn(ROCKSDB_POOL, this.getRange(null, range, false, smallRange), executing(oldValue -> {
				try (var writeOptions = new LLWriteOptions()) {
					db.delete(writeOptions, oldValue.getKey());
				} catch (RocksDBException ex) {
					throw new CompletionException(new DBException("Failed to write range", ex));
				}
			}));

			collectOn(ROCKSDB_POOL, entries, executing(entry -> {
				if (entry.getKey() != null && entry.getValue() != null) {
					this.putInternal(entry.getKey(), entry.getValue());
				}
			}));
		}
	}

	private void deleteSmallRangeWriteBatch(WriteBatch writeBatch, LLRange range)
			throws RocksDBException {
		try (var readOpts = LLUtils.generateCustomReadOptions(null, false, isBoundedRange(range), true)) {
			try (var rocksIterator = db.newIterator(readOpts, range.getMin(), range.getMax())) {
				if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
					rocksIterator.seekTo(range.getMin());
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

	public void clear() {
		assert !LLUtils.isInNonBlockingThread() : "Called clear in a nonblocking thread";
		boolean shouldCompactLater = false;
		try (var writeOptions = new LLWriteOptions();
				var readOpts = LLUtils.generateCustomReadOptions(null, false, false, false)) {
			if (VERIFY_CHECKSUMS_WHEN_NOT_NEEDED) {
				readOpts.setVerifyChecksums(true);
			}
			// readOpts.setIgnoreRangeDeletions(true);
			if (LLUtils.MANUAL_READAHEAD) {
				readOpts.setReadaheadSize(32 * 1024); // 32KiB
			}
			try (CappedWriteBatch writeBatch = new CappedWriteBatch(db,
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
							lastDeletedKey = rocksIterator.key().clone();
							writeBatch.deleteRange(cfh, FIRST_KEY, lastDeletedKey);
							writeBatch.delete(cfh, lastDeletedKey);
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
		} catch (RocksDBException ex) {
			throw new DBException("Failed to clear", ex);
		}
	}

	@Override
	public long sizeRange(@Nullable LLSnapshot snapshot, LLRange range, boolean fast) {
		try {
			assert !LLUtils.isInNonBlockingThread() : "Called sizeRange in a nonblocking thread";
			if (range.isAll()) {
				return fast ? fastSizeAll(snapshot) : exactSizeAll(snapshot);
			} else {
				try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNew(snapshot),
						false,
						isBoundedRange(range),
						false
				)) {
					readOpts.setFillCache(false);
					readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);
					if (fast) {
						readOpts.setIgnoreRangeDeletions(true);

					}
					try (var rocksIterator = db.newIterator(readOpts, range.getMin(), range.getMax())) {
						if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
							rocksIterator.seekTo(range.getMin());
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
				}
			}
		} catch (RocksDBException ex) {
			throw new DBException("Failed to get size of range", ex);
		}
	}

	@Override
	public LLEntry getOne(@Nullable LLSnapshot snapshot, LLRange range) {
		try {
			assert !LLUtils.isInNonBlockingThread() : "Called getOne in a nonblocking thread";
			try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNew(snapshot), true, true, true)) {
				try (var rocksIterator = db.newIterator(readOpts, range.getMin(), range.getMax())) {
					if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
						rocksIterator.seekTo(range.getMin());
					} else {
						rocksIterator.seekToFirst();
					}
					if (rocksIterator.isValid()) {
						var keyView = rocksIterator.keyBuf();
						var valueView = rocksIterator.valueBuf();
						return LLEntry.copyOf(keyView, valueView);
					} else {
						return null;
					}
				}
			}
		} catch (RocksDBException ex) {
			throw new DBException("Failed to get one entry", ex);
		}
	}

	@Override
	public Buf getOneKey(@Nullable LLSnapshot snapshot, LLRange range) {
		try {
			assert !LLUtils.isInNonBlockingThread() : "Called getOneKey in a nonblocking thread";
			try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNew(snapshot), true, true, true)) {
				try (var rocksIterator = db.newIterator(readOpts, range.getMin(), range.getMax())) {
					if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
						rocksIterator.seekTo(range.getMin());
					} else {
						rocksIterator.seekToFirst();
					}
					if (rocksIterator.isValid()) {
						return rocksIterator.keyBuf();
					} else {
						return null;
					}
				}
			}
		} catch (RocksDBException ex) {
			throw new DBException("Failed to get one key", ex);
		}
	}

	private long fastSizeAll(@Nullable LLSnapshot snapshot) throws RocksDBException {
		try (var rocksdbSnapshot = generateReadOptionsOrNew(snapshot)) {
			if (USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS || rocksdbSnapshot.snapshot() == null) {
				try {
					if (USE_NUM_ENTRIES_PRECISE_COUNTER) {
						return getRocksDBNumEntries();
					}
					return db.getLongProperty("rocksdb.estimate-num-keys");
				} catch (RocksDBException e) {
					logger.error(MARKER_ROCKSDB, "Failed to get RocksDB estimated keys count property", e);
					return 0;
				}
			} else if (USE_NUM_ENTRIES_PRECISE_COUNTER && snapshot == null) {
				return getRocksDBNumEntries();
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

	private long getRocksDBNumEntries() {
		try {
			return db.getNumEntries();
		} catch (RocksDBException ex) {
			throw new IllegalStateException("Failed to read exact size", ex);
		}
	}

	private long exactSizeAll(@Nullable LLSnapshot snapshot) {
		if (LLUtils.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called exactSizeAll in a nonblocking thread");
		}
		try (var readOpts = LLUtils.generateCustomReadOptions(generateReadOptionsOrNew(snapshot), false, false, false)) {
			if (LLUtils.MANUAL_READAHEAD) {
				readOpts.setReadaheadSize(128 * 1024); // 128KiB
			}
			readOpts.setVerifyChecksums(VERIFY_CHECKSUMS_WHEN_NOT_NEEDED);

			if (PARALLEL_EXACT_SIZE) {
				return collectOn(ROCKSDB_POOL, IntStream
						.range(-1, LLUtils.LEXICONOGRAPHIC_ITERATION_SEEKS.length)
						.mapToObj(idx -> Pair.of(idx == -1 ? new byte[0] : LLUtils.LEXICONOGRAPHIC_ITERATION_SEEKS[idx],
								idx + 1 >= LLUtils.LEXICONOGRAPHIC_ITERATION_SEEKS.length ? null
										: LLUtils.LEXICONOGRAPHIC_ITERATION_SEEKS[idx + 1]
						)).map(range -> {
							long partialCount = 0;
							try (var rangeReadOpts = readOpts.copy()) {
								try {
									try (var rocksIterator = db.newIterator(rangeReadOpts,
											wrapNullable(range.getKey()),
											wrapNullable(range.getValue())
									)) {
										rocksIterator.seekToFirst();
										while (rocksIterator.isValid()) {
											partialCount++;
											rocksIterator.next();
										}
										return partialCount;
									}
								} catch (RocksDBException ex) {
									throw new CompletionException(new IOException("Failed to get size", ex));
								}
							}
						}), fastSummingLong());
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
	public LLEntry removeOne(LLRange range) {
		assert !LLUtils.isInNonBlockingThread() : "Called removeOne in a nonblocking thread";
		try (var readOpts = new LLReadOptions();
				var writeOpts = new LLWriteOptions()) {
			try (var rocksIterator = db.newIterator(readOpts, range.getMin(), range.getMax())) {
				if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
					rocksIterator.seekTo(range.getMin());
				} else {
					rocksIterator.seekToFirst();
				}
				if (!rocksIterator.isValid()) {
					return null;
				}
				Buf key = rocksIterator.keyBuf().copy();
				Buf value = rocksIterator.valueBuf().copy();
				db.delete(writeOpts, key);
				return LLEntry.of(key, value);
			} catch (RocksDBException e) {
				throw new DBException("Failed to remove key", e);
			}
		}
	}

}
