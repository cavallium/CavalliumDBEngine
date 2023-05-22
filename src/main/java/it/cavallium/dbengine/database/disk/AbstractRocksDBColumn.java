package it.cavallium.dbengine.database.disk;

import static java.util.Objects.requireNonNull;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.RepeatedElementList;
import it.cavallium.dbengine.database.disk.rocksdb.LLReadOptions;
import it.cavallium.dbengine.database.disk.rocksdb.LLSlice;
import it.cavallium.dbengine.database.disk.rocksdb.LLWriteOptions;
import it.cavallium.dbengine.database.disk.rocksdb.RocksIteratorObj;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.utils.SimpleResource;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Holder;
import org.rocksdb.KeyMayExist;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksObject;
import org.rocksdb.TableProperties;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteBatch;

public sealed abstract class AbstractRocksDBColumn<T extends RocksDB> implements RocksDBColumn
		permits StandardRocksDBColumn, OptimisticRocksDBColumn, PessimisticRocksDBColumn {
	private static final byte[] NO_DATA = new byte[0];
	protected static final UpdateAtomicResult RESULT_NOTHING = new UpdateAtomicResultNothing();

	protected final Logger logger = LogManager.getLogger(this.getClass());

	private final T db;
	private final ColumnFamilyHandle cfh;

	protected final MeterRegistry meterRegistry;
	protected final StampedLock closeLock;
	protected final String columnName;

	protected final DistributionSummary keyBufferSize;
	protected final DistributionSummary readValueNotFoundWithoutBloomBufferSize;
	protected final DistributionSummary readValueNotFoundWithBloomBufferSize;
	protected final DistributionSummary readValueNotFoundWithMayExistBloomBufferSize;
	protected final DistributionSummary readValueFoundWithBloomUncachedBufferSize;
	protected final DistributionSummary readValueFoundWithBloomCacheBufferSize;
	protected final DistributionSummary readValueFoundWithBloomSimpleBufferSize;
	protected final DistributionSummary readValueFoundWithoutBloomBufferSize;
	protected final DistributionSummary writeValueBufferSize;
	protected final DistributionSummary readAttempts;

	private final Counter startedIterSeek;
	private final Counter endedIterSeek;
	private final Timer iterSeekTime;
	private final Counter startedIterNext;
	private final Counter endedIterNext;
	private final Timer iterNextTime;

	private final Counter startedUpdate;
	private final Counter endedUpdate;
	private final Timer updateAddedTime;
	private final Timer updateReplacedTime;
	private final Timer updateRemovedTime;
	private final Timer updateUnchangedTime;
	private final DBColumnKeyMayExistGetter keyMayExistGetter;
	private final IteratorMetrics iteratorMetrics;

	public AbstractRocksDBColumn(T db,
			String databaseName,
			ColumnFamilyHandle cfh,
			MeterRegistry meterRegistry,
			StampedLock closeLock) {
		this.db = db;
		this.cfh = cfh;
		String columnName;
		try {
			columnName = new String(cfh.getName(), StandardCharsets.UTF_8);
		} catch (RocksDBException e) {
			throw new IllegalStateException(e);
		}
		this.columnName = columnName;
		this.meterRegistry = meterRegistry;
		this.closeLock = closeLock;

		this.keyBufferSize = DistributionSummary
				.builder("buffer.size.distribution")
				.publishPercentiles(0.2, 0.5, 0.95)
				.baseUnit("bytes")
				.scale(1)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "buffer.type", "key")
				.register(meterRegistry);
		this.readValueNotFoundWithoutBloomBufferSize = DistributionSummary
				.builder("buffer.size.distribution")
				.publishPercentiles(0.2, 0.5, 0.95)
				.baseUnit("bytes")
				.scale(1)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "buffer.type", "val.read", "found", "false", "bloom", "disabled")
				.register(meterRegistry);
		this.readValueNotFoundWithBloomBufferSize = DistributionSummary
				.builder("buffer.size.distribution")
				.publishPercentiles(0.2, 0.5, 0.95)
				.baseUnit("bytes")
				.scale(1)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "buffer.type", "val.read", "found", "false", "bloom", "enabled", "bloom.mayexist", "false")
				.register(meterRegistry);
		this.readValueNotFoundWithMayExistBloomBufferSize = DistributionSummary
				.builder("buffer.size.distribution")
				.publishPercentiles(0.2, 0.5, 0.95)
				.baseUnit("bytes")
				.scale(1)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "buffer.type", "val.read", "found", "false", "bloom", "enabled", "bloom.mayexist", "true", "bloom.mayexist.result", "notexists", "bloom.mayexist.cached", "false")
				.register(meterRegistry);
		this.readValueFoundWithBloomUncachedBufferSize = DistributionSummary
				.builder("buffer.size.distribution")
				.publishPercentiles(0.2, 0.5, 0.95)
				.baseUnit("bytes")
				.scale(1)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "buffer.type", "val.read", "found", "true", "bloom", "enabled", "bloom.mayexist", "true", "bloom.mayexist.result", "exists", "bloom.mayexist.cached", "false")
				.register(meterRegistry);
		this.readValueFoundWithBloomCacheBufferSize = DistributionSummary
				.builder("buffer.size.distribution")
				.publishPercentiles(0.2, 0.5, 0.95)
				.baseUnit("bytes")
				.scale(1)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "buffer.type", "val.read", "found", "true", "bloom", "enabled", "bloom.mayexist", "true", "bloom.mayexist.result", "exists", "bloom.mayexist.cached", "true")
				.register(meterRegistry);
		this.readValueFoundWithBloomSimpleBufferSize = DistributionSummary
				.builder("buffer.size.distribution")
				.publishPercentiles(0.2, 0.5, 0.95)
				.baseUnit("bytes")
				.scale(1)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "buffer.type", "val.read", "found", "true", "bloom", "enabled", "bloom.mayexist", "true", "bloom.mayexist.result", "exists", "bloom.mayexist.cached", "false")
				.register(meterRegistry);
		this.readValueFoundWithoutBloomBufferSize = DistributionSummary
				.builder("buffer.size.distribution")
				.publishPercentiles(0.2, 0.5, 0.95)
				.baseUnit("bytes")
				.scale(1)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "buffer.type", "val.read", "found", "true", "bloom", "disabled")
				.register(meterRegistry);
		this.writeValueBufferSize = DistributionSummary
				.builder("buffer.size.distribution")
				.publishPercentiles(0.2, 0.5, 0.95)
				.baseUnit("bytes")
				.scale(1)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "buffer.type", "val.write")
				.register(meterRegistry);
		this.readAttempts = DistributionSummary
				.builder("db.read.attempts.distribution")
				.publishPercentiles(0.2, 0.5, 0.95)
				.baseUnit("times")
				.scale(1)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName)
				.register(meterRegistry);

		this.startedIterSeek = meterRegistry.counter("db.read.iter.seek.started.counter", "db.name", databaseName, "db.column", columnName);
		this.endedIterSeek = meterRegistry.counter("db.read.iter.seek.ended.counter", "db.name", databaseName, "db.column", columnName);
		this.iterSeekTime = Timer
				.builder("db.read.iter.seek.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName)
				.register(meterRegistry);
		this.startedIterNext = meterRegistry.counter("db.read.iter.next.started.counter", "db.name", databaseName, "db.column", columnName);
		this.endedIterNext = meterRegistry.counter("db.read.iter.next.ended.counter", "db.name", databaseName, "db.column", columnName);
		this.iterNextTime = Timer
				.builder("db.read.iter.next.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName)
				.register(meterRegistry);

		this.startedUpdate = meterRegistry.counter("db.write.update.started.counter", "db.name", databaseName, "db.column", columnName);
		this.endedUpdate = meterRegistry.counter("db.write.update.ended.counter", "db.name", databaseName, "db.column", columnName);
		this.updateAddedTime = Timer
				.builder("db.write.update.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "update.type", "added")
				.register(meterRegistry);
		this.updateReplacedTime = Timer
				.builder("db.write.update.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "update.type", "replaced")
				.register(meterRegistry);
		this.updateRemovedTime = Timer
				.builder("db.write.update.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "update.type", "removed")
				.register(meterRegistry);
		this.updateUnchangedTime = Timer
				.builder("db.write.update.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName, "update.type", "unchanged")
				.register(meterRegistry);
		this.iteratorMetrics = new IteratorMetrics(this.startedIterSeek,
				this.endedIterSeek,
				this.iterSeekTime,
				this.startedIterNext,
				this.endedIterNext,
				this.iterNextTime);
		this.keyMayExistGetter = new DBColumnKeyMayExistGetter();
	}

	/**
	 * This method should not modify or move the writerIndex/readerIndex of the key
	 */
	static void setIterateBound(LLReadOptions readOpts, IterateBound boundType, Buf key) {
		requireNonNull(key);
		LLSlice slice;
		slice = LLSlice.of(requireNonNull(LLUtils.asArray(key)));
		if (boundType == IterateBound.LOWER) {
			readOpts.setIterateLowerBound(slice);
		} else {
			readOpts.setIterateUpperBound(slice);
		}
	}

	/**
	 * This method should not modify or move the writerIndex/readerIndex of the buffers inside the range
	 */
	@Override
	@NotNull
	public RocksIteratorObj newRocksIterator(LLReadOptions readOptions, LLRange range, boolean reverse)
			throws RocksDBException {
		assert !LLUtils.isInNonBlockingThread() : "Called getRocksIterator in a nonblocking thread";
		var rocksIterator = this.newIterator(readOptions, range.getMin(), range.getMax());
		try {
			if (reverse) {
				if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMax()) {
					rocksIterator.seekFrom(range.getMax());
				} else {
					rocksIterator.seekToLast();
				}
			} else {
				if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
					rocksIterator.seekTo(range.getMin());
				} else {
					rocksIterator.seekToFirst();
				}
			}
			return rocksIterator;
		} catch (Throwable ex) {
			rocksIterator.close();
			throw ex;
		}
	}

	protected T getDb() {
		return db;
	}

	protected ColumnFamilyHandle getCfh() {
		return cfh;
	}

	protected void ensureOpen() {
		RocksDBUtils.ensureOpen(db, cfh);
	}

	protected void ensureOwned(AbstractImmutableNativeReference rocksObject) {
		RocksDBUtils.ensureOwned(rocksObject);
	}


	protected void ensureOwned(RocksObject rocksObject) {
		RocksDBUtils.ensureOwned(rocksObject);
	}


	protected void ensureOwned(SimpleResource simpleResource) {
		RocksDBUtils.ensureOwned(simpleResource);
	}

	@Override
	public @Nullable Buf get(@NotNull LLReadOptions readOptions, Buf key) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			return keyMayExistGetter.get(readOptions, key);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void put(@NotNull LLWriteOptions writeOptions, Buf key, Buf value) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(writeOptions);
			this.keyBufferSize.record(key.size());
			this.writeValueBufferSize.record(value.size());
			db.put(cfh, writeOptions.getUnsafe(), LLUtils.asArray(key), LLUtils.asArray(value));
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public boolean exists(@NotNull LLReadOptions readOptions, Buf key) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			int size = RocksDB.NOT_FOUND;
			byte[] keyBytes = LLUtils.asArray(key);
			Holder<byte[]> data = new Holder<>();
			boolean mayExistHit = false;
			if (db.keyMayExist(cfh, readOptions.getUnsafe(), keyBytes, data)) {
				mayExistHit = true;
				if (data.getValue() != null) {
					size = data.getValue().length;
				} else {
					size = db.get(cfh, readOptions.getUnsafe(), keyBytes, NO_DATA);
				}
			}
			boolean found = size != RocksDB.NOT_FOUND;
			if (found) {
				readValueFoundWithBloomSimpleBufferSize.record(size);
			} else {
				if (mayExistHit) {
					readValueNotFoundWithMayExistBloomBufferSize.record(0);
				} else {
					readValueNotFoundWithBloomBufferSize.record(0);
				}
			}
			return found;
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public boolean mayExists(@NotNull LLReadOptions readOptions, Buf key) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			byte[] keyBytes = LLUtils.asArray(key);
			return db.keyMayExist(cfh, readOptions.getUnsafe(), keyBytes, null);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void delete(LLWriteOptions writeOptions, Buf key) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(writeOptions);
			keyBufferSize.record(key.size());
			db.delete(cfh, writeOptions.getUnsafe(), LLUtils.asArray(key));
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void delete(LLWriteOptions writeOptions, byte[] key) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(writeOptions);
			keyBufferSize.record(key.length);
			db.delete(cfh, writeOptions.getUnsafe(), key);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public List<byte[]> multiGetAsList(LLReadOptions readOptions, List<byte[]> keys) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			for (byte[] key : keys) {
				keyBufferSize.record(key.length);
			}
			var columnFamilyHandles = new RepeatedElementList<>(cfh, keys.size());
			return db.multiGetAsList(readOptions.getUnsafe(), columnFamilyHandles, keys);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void suggestCompactRange() throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			db.suggestCompactRange(cfh);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void compactRange(byte[] begin, byte[] end, CompactRangeOptions options) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			 ensureOpen();
			 ensureOwned(options);
			db.compactRange(cfh, begin, end, options);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void flush(FlushOptions options) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(options);
			db.flush(options, cfh);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void flushWal(boolean sync) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			db.flushWal(sync);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public long getLongProperty(String property) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			return db.getLongProperty(cfh, property);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public long getNumEntries() throws RocksDBException {
		Map<String, TableProperties> props = db.getPropertiesOfAllTables(cfh);
		long entries = 0;
		for (TableProperties tableProperties : props.values()) {
			entries += tableProperties.getNumEntries();
		}
		return entries;
	}

	@Override
	public void write(LLWriteOptions writeOptions, WriteBatch writeBatch) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(writeOptions);
			ensureOwned(writeBatch);
			db.write(writeOptions.getUnsafe(), writeBatch);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	/**
	 * @return true if committed successfully
	 */
	protected abstract boolean commitOptimistically(Transaction tx) throws RocksDBException;

	protected abstract Transaction beginTransaction(@NotNull LLWriteOptions writeOptions,
			TransactionOptions txOpts);

	@Override
	public final @NotNull UpdateAtomicResult updateAtomic(@NotNull LLReadOptions readOptions,
			@NotNull LLWriteOptions writeOptions,
			Buf key,
			SerializationFunction<@Nullable Buf, @Nullable Buf> updater,
			UpdateAtomicResultMode returnMode) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			try {
				keyBufferSize.record(key.size());
				startedUpdate.increment();
				return updateAtomicImpl(readOptions, writeOptions, key, updater, returnMode);
			} finally {
				endedUpdate.increment();
			}
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	protected final void recordAtomicUpdateTime(boolean changed, boolean prevSet, boolean newSet, long initTime) {
		long duration = System.nanoTime() - initTime;
		Timer timer;
		if (changed) {
			if (prevSet && newSet) {
				timer = updateReplacedTime;
			} else if (newSet) {
				timer = updateAddedTime;
			} else {
				timer = updateRemovedTime;
			}
		} else {
			timer = updateUnchangedTime;
		}

		timer.record(duration, TimeUnit.NANOSECONDS);
	}

	protected abstract @NotNull UpdateAtomicResult updateAtomicImpl(@NotNull LLReadOptions readOptions,
			@NotNull LLWriteOptions writeOptions,
			Buf key,
			SerializationFunction<@Nullable Buf, @Nullable Buf> updater,
			UpdateAtomicResultMode returnMode);

	@Override
	@NotNull
	public RocksIteratorObj newIterator(@NotNull LLReadOptions readOptions,
			@Nullable Buf min,
			@Nullable Buf max) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			setIterateBound(readOptions, IterateBound.LOWER, min);
			setIterateBound(readOptions, IterateBound.UPPER, max);
			return readOptions.newIterator(db, cfh, iteratorMetrics);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	protected int getLevels() {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			return RocksDBUtils.getLevels(db, cfh);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public final void forceCompaction(int volumeId) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			RocksDBUtils.forceCompaction(db, db.getName(), cfh, volumeId, logger);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public ColumnFamilyHandle getColumnFamilyHandle() {
		return cfh;
	}

	public MeterRegistry getMeterRegistry() {
		return meterRegistry;
	}

	public Timer getIterNextTime() {
		return iterNextTime;
	}

	public Counter getStartedIterNext() {
		return startedIterNext;
	}

	public Counter getEndedIterNext() {
		return endedIterNext;
	}

	private class DBColumnKeyMayExistGetter extends KeyMayExistGetter {

		@Override
		protected KeyMayExist keyMayExist(LLReadOptions readOptions, ByteBuffer key, ByteBuffer value) {
			return db.keyMayExist(cfh, readOptions.getUnsafe(), key, value);
		}

		@Override
		protected boolean keyMayExist(LLReadOptions readOptions, byte[] key, @Nullable Holder<byte[]> valueHolder) {
			return db.keyMayExist(cfh, readOptions.getUnsafe(), key, valueHolder);
		}

		@Override
		protected int get(LLReadOptions readOptions, ByteBuffer key, ByteBuffer value) throws RocksDBException {
			return db.get(cfh, readOptions.getUnsafe(), key, value);
		}

		@Override
		protected byte[] get(LLReadOptions readOptions, byte[] key) throws RocksDBException, IllegalArgumentException {
			return db.get(cfh, readOptions.getUnsafe(), key);
		}

		@Override
		protected void recordReadValueNotFoundWithMayExistBloomBufferSize(int value) {
			readValueNotFoundWithMayExistBloomBufferSize.record(value);
		}

		@Override
		protected void recordReadValueFoundWithBloomUncachedBufferSize(int value) {
			readValueFoundWithBloomUncachedBufferSize.record(value);
		}

		@Override
		protected void recordReadValueFoundWithBloomCacheBufferSize(int value) {
			readValueFoundWithBloomCacheBufferSize.record(value);
		}

		@Override
		protected void recordReadAttempts(int value) {
			readAttempts.record(value);
		}

		@Override
		protected void recordReadValueNotFoundWithBloomBufferSize(int value) {
			readValueNotFoundWithBloomBufferSize.record(value);
		}

		@Override
		protected void recordKeyBufferSize(int value) {
			keyBufferSize.record(value);
		}
	}
}
