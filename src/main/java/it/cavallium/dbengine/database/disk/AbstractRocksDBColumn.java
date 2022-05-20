package it.cavallium.dbengine.database.disk;

import static io.netty5.buffer.api.StandardAllocationTypes.OFF_HEAP;
import static it.cavallium.dbengine.database.LLUtils.INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES;
import static it.cavallium.dbengine.database.LLUtils.isReadOnlyDirect;
import static java.util.Objects.requireNonNull;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithValue;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithoutValue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.DefaultBufferAllocators;
import io.netty5.buffer.api.ReadableComponent;
import io.netty5.buffer.api.WritableComponent;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.RepeatedElementList;
import it.cavallium.dbengine.database.disk.rocksdb.RocksIteratorObj;
import it.cavallium.dbengine.database.serialization.SerializationException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.AbstractSlice;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.DirectSlice;
import org.rocksdb.FlushOptions;
import org.rocksdb.Holder;
import org.rocksdb.KeyMayExist.KeyMayExistEnum;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksObject;
import org.rocksdb.Slice;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import reactor.core.scheduler.Schedulers;

public sealed abstract class AbstractRocksDBColumn<T extends RocksDB> implements RocksDBColumn
		permits StandardRocksDBColumn, OptimisticRocksDBColumn, PessimisticRocksDBColumn {

	/**
	 * Default: true
	 */
	private static final boolean USE_DIRECT_BUFFER_BOUNDS = true;
	private static final byte[] NO_DATA = new byte[0];
	protected static final UpdateAtomicResult RESULT_NOTHING = new UpdateAtomicResultNothing();

	protected final Logger logger = LogManager.getLogger(this.getClass());

	private final T db;
	private final boolean nettyDirect;
	private final BufferAllocator alloc;
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

	public AbstractRocksDBColumn(T db,
			boolean nettyDirect,
			BufferAllocator alloc,
			String databaseName,
			ColumnFamilyHandle cfh,
			MeterRegistry meterRegistry,
			StampedLock closeLock) {
		this.db = db;
		this.nettyDirect = nettyDirect && alloc.getAllocationType() == OFF_HEAP;
		this.alloc = alloc;
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
	}

	/**
	 * This method should not modify or move the writerIndex/readerIndex of the key
	 */
	static AbstractSlice<?> setIterateBound(boolean allowNettyDirect,
			ReadOptions readOpts, IterateBound boundType, Buffer key) {
		requireNonNull(key);
		AbstractSlice<?> slice;
		if (allowNettyDirect && USE_DIRECT_BUFFER_BOUNDS && isReadOnlyDirect(key)) {
			ByteBuffer keyInternalByteBuffer = ((ReadableComponent) key).readableBuffer();
			assert keyInternalByteBuffer.position() == 0;
			slice = new DirectSlice(keyInternalByteBuffer, key.readableBytes());
			assert slice.size() == key.readableBytes();
		} else {
			slice = new Slice(requireNonNull(LLUtils.toArray(key)));
		}
		if (boundType == IterateBound.LOWER) {
			readOpts.setIterateLowerBound(slice);
		} else {
			readOpts.setIterateUpperBound(slice);
		}
		return slice;
	}

	static Slice newEmptyReleasableSlice() {
		var arr = new byte[0];

		return new Slice(arr);
	}

	/**
	 * This method should not modify or move the writerIndex/readerIndex of the buffers inside the range
	 */
	@NotNull
	public RocksIteratorObj newRocksIterator(boolean allowNettyDirect,
			ReadOptions readOptions,
			LLRange range,
			boolean reverse) throws RocksDBException {
		assert !Schedulers.isInNonBlockingThread() : "Called getRocksIterator in a nonblocking thread";
		var rocksIterator = this.newIterator(readOptions, range.getMinUnsafe(), range.getMaxUnsafe());
		try {
			if (reverse) {
				if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMax()) {
					rocksIterator.seekFrom(range.getMaxUnsafe());
				} else {
					rocksIterator.seekToLast();
				}
			} else {
				if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
					rocksIterator.seekTo(range.getMinUnsafe());
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

	protected void ensureOwned(Buffer buffer) {
		if (buffer != null && !buffer.isAccessible()) {
			throw new IllegalStateException("Buffer is not accessible");
		}
	}

	@Override
	public @Nullable Buffer get(@NotNull ReadOptions readOptions, Buffer key) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			keyBufferSize.record(key.readableBytes());
			int readAttemptsCount = 0;
			try {
				if (nettyDirect) {
					// Get the key nio buffer to pass to RocksDB
					ByteBuffer keyNioBuffer;
					boolean mustCloseKey;
					{
						if (!LLUtils.isReadOnlyDirect(key)) {
							// If the nio buffer is not available, copy the netty buffer into a new direct buffer
							mustCloseKey = true;
							var directKey = DefaultBufferAllocators.offHeapAllocator().allocate(key.readableBytes());
							key.copyInto(key.readerOffset(), directKey, 0, key.readableBytes());
							key = directKey;
						} else {
							mustCloseKey = false;
						}
						keyNioBuffer = ((ReadableComponent) key).readableBuffer();
						assert keyNioBuffer.isDirect();
						assert keyNioBuffer.limit() == key.readableBytes();
					}

					try {
						// Create a direct result buffer because RocksDB works only with direct buffers
						var resultBuffer = alloc.allocate(INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES);
						try {
							assert resultBuffer.readerOffset() == 0;
							assert resultBuffer.writerOffset() == 0;
							var resultWritable = ((WritableComponent) resultBuffer).writableBuffer();

							var keyMayExist = db.keyMayExist(cfh, readOptions, keyNioBuffer.rewind(),
									resultWritable.clear());
							KeyMayExistEnum keyMayExistState = keyMayExist.exists;
							int keyMayExistValueLength = keyMayExist.valueLength;
							// At the beginning, size reflects the expected size, then it becomes the real data size
							int size = keyMayExistState == kExistsWithValue ? keyMayExistValueLength : -1;
							boolean isKExistsWithoutValue = false;
							switch (keyMayExistState) {
								case kNotExist: {
									readValueNotFoundWithBloomBufferSize.record(0);
									resultBuffer.close();
									return null;
								}
								// todo: kExistsWithValue is not reliable (read below),
								//  in some cases it should be treated as kExistsWithoutValue
								case kExistsWithValue:
								case kExistsWithoutValue: {
									if (keyMayExistState == kExistsWithoutValue) {
										isKExistsWithoutValue = true;
									} else {
										// todo: "size == 0 || resultWritable.limit() == 0" is checked because keyMayExist is broken,
										//  and sometimes it returns an empty array, as if it exists
										if (size == 0 || resultWritable.limit() == 0) {
											isKExistsWithoutValue = true;
										}
									}
									if (isKExistsWithoutValue) {
										assert keyMayExistValueLength == 0;
										resultWritable.clear();
										readAttemptsCount++;
										// real data size
										size = db.get(cfh, readOptions, keyNioBuffer.rewind(), resultWritable.clear());
										if (size == RocksDB.NOT_FOUND) {
											resultBuffer.close();
											readValueNotFoundWithMayExistBloomBufferSize.record(0);
											return null;
										}
									}
								}
								default: {
									// real data size
									assert size >= 0;
									if (size <= resultWritable.limit()) {
										if (isKExistsWithoutValue) {
											readValueFoundWithBloomUncachedBufferSize.record(size);
										} else {
											readValueFoundWithBloomCacheBufferSize.record(size);
										}
										assert size == resultWritable.limit();
										return resultBuffer.writerOffset(resultWritable.limit());
									} else {
										resultBuffer.ensureWritable(size);
										resultWritable = ((WritableComponent) resultBuffer).writableBuffer();
										assert resultBuffer.readerOffset() == 0;
										assert resultBuffer.writerOffset() == 0;

										readAttemptsCount++;
										size = db.get(cfh, readOptions, keyNioBuffer.rewind(), resultWritable.clear());
										if (size == RocksDB.NOT_FOUND) {
											readValueNotFoundWithMayExistBloomBufferSize.record(0);
											resultBuffer.close();
											return null;
										}
										assert size == resultWritable.limit();
										if (isKExistsWithoutValue) {
											readValueFoundWithBloomUncachedBufferSize.record(size);
										} else {
											readValueFoundWithBloomCacheBufferSize.record(size);
										}
										return resultBuffer.writerOffset(resultWritable.limit());
									}
								}
							}
						} catch (Throwable t) {
							resultBuffer.close();
							throw t;
						}
					} finally {
						if (mustCloseKey) {
							key.close();
						}
					}
				} else {
					byte[] keyArray = LLUtils.toArray(key);
					requireNonNull(keyArray);
					Holder<byte[]> data = new Holder<>();
					if (db.keyMayExist(cfh, readOptions, keyArray, data)) {
						// todo: "data.getValue().length > 0" is checked because keyMayExist is broken, and sometimes it
						//  returns an empty array, as if it exists
						if (data.getValue() != null && data.getValue().length > 0) {
							readValueFoundWithBloomCacheBufferSize.record(data.getValue().length);
							return LLUtils.fromByteArray(alloc, data.getValue());
						} else {
							readAttemptsCount++;
							byte[] result = db.get(cfh, readOptions, keyArray);
							if (result == null) {
								if (data.getValue() != null) {
									readValueNotFoundWithBloomBufferSize.record(0);
								} else {
									readValueNotFoundWithMayExistBloomBufferSize.record(0);
								}
								return null;
							} else {
								readValueFoundWithBloomUncachedBufferSize.record(0);
								return LLUtils.fromByteArray(alloc, result);
							}
						}
					} else {
						readValueNotFoundWithBloomBufferSize.record(0);
						return null;
					}
				}
			} finally {
				readAttempts.record(readAttemptsCount);
			}
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void put(@NotNull WriteOptions writeOptions, Buffer key, Buffer value) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(writeOptions);
			assert key.isAccessible();
			assert value.isAccessible();
			this.keyBufferSize.record(key.readableBytes());
			this.writeValueBufferSize.record(value.readableBytes());
			if (nettyDirect) {
				// Get the key nio buffer to pass to RocksDB
				ByteBuffer keyNioBuffer;
				boolean mustCloseKey;
				{
					if (!LLUtils.isReadOnlyDirect(key)) {
						// If the nio buffer is not available, copy the netty buffer into a new direct buffer
						mustCloseKey = true;
						var directKey = DefaultBufferAllocators.offHeapAllocator().allocate(key.readableBytes());
						key.copyInto(key.readerOffset(), directKey, 0, key.readableBytes());
						key = directKey;
					} else {
						mustCloseKey = false;
					}
					keyNioBuffer = ((ReadableComponent) key).readableBuffer();
					assert keyNioBuffer.isDirect();
					assert keyNioBuffer.limit() == key.readableBytes();
				}
				try {
					// Get the value nio buffer to pass to RocksDB
					ByteBuffer valueNioBuffer;
					boolean mustCloseValue;
					{
						if (!LLUtils.isReadOnlyDirect(value)) {
							// If the nio buffer is not available, copy the netty buffer into a new direct buffer
							mustCloseValue = true;
							var directValue = DefaultBufferAllocators.offHeapAllocator().allocate(value.readableBytes());
							value.copyInto(value.readerOffset(), directValue, 0, value.readableBytes());
							value = directValue;
						} else {
							mustCloseValue = false;
						}
						valueNioBuffer = ((ReadableComponent) value).readableBuffer();
						assert valueNioBuffer.isDirect();
						assert valueNioBuffer.limit() == value.readableBytes();
					}

					try {
						db.put(cfh, writeOptions, keyNioBuffer, valueNioBuffer);
					} finally {
						if (mustCloseValue) {
							value.close();
						}
					}
				} finally {
					if (mustCloseKey) {
						key.close();
					}
				}
			} else {
				db.put(cfh, writeOptions, LLUtils.toArray(key), LLUtils.toArray(value));
			}
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public boolean exists(@NotNull ReadOptions readOptions, Buffer key) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			if (nettyDirect) {
				// Get the key nio buffer to pass to RocksDB
				ByteBuffer keyNioBuffer;
				boolean mustCloseKey;
				{
					if (!LLUtils.isReadOnlyDirect(key)) {
						// If the nio buffer is not available, copy the netty buffer into a new direct buffer
						mustCloseKey = true;
						var directKey = DefaultBufferAllocators.offHeapAllocator().allocate(key.readableBytes());
						key.copyInto(key.readerOffset(), directKey, 0, key.readableBytes());
						key = directKey;
					} else {
						mustCloseKey = false;
					}
					keyNioBuffer = ((ReadableComponent) key).readableBuffer();
					assert keyNioBuffer.isDirect();
					assert keyNioBuffer.limit() == key.readableBytes();
				}
				try {
					if (db.keyMayExist(cfh, keyNioBuffer)) {
						int size = db.get(cfh, readOptions, keyNioBuffer.position(0), LLUtils.EMPTY_BYTE_BUFFER);
						boolean found = size != RocksDB.NOT_FOUND;
						if (found) {
							readValueFoundWithBloomSimpleBufferSize.record(size);
							return true;
						} else {
							readValueNotFoundWithMayExistBloomBufferSize.record(0);
							return false;
						}
					} else {
						readValueNotFoundWithBloomBufferSize.record(0);
						return false;
					}
				} finally {
					if (mustCloseKey) {
						key.close();
					}
				}
			} else {
				int size = RocksDB.NOT_FOUND;
				byte[] keyBytes = LLUtils.toArray(key);
				Holder<byte[]> data = new Holder<>();
				boolean mayExistHit = false;
				if (db.keyMayExist(cfh, readOptions, keyBytes, data)) {
					mayExistHit = true;
					if (data.getValue() != null) {
						size = data.getValue().length;
					} else {
						size = db.get(cfh, readOptions, keyBytes, NO_DATA);
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
			}
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public boolean mayExists(@NotNull ReadOptions readOptions, Buffer key) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			if (nettyDirect) {
				// Get the key nio buffer to pass to RocksDB
				ByteBuffer keyNioBuffer;
				boolean mustCloseKey;
				{
					if (!LLUtils.isReadOnlyDirect(key)) {
						// If the nio buffer is not available, copy the netty buffer into a new direct buffer
						mustCloseKey = true;
						var directKey = DefaultBufferAllocators.offHeapAllocator().allocate(key.readableBytes());
						key.copyInto(key.readerOffset(), directKey, 0, key.readableBytes());
						key = directKey;
					} else {
						mustCloseKey = false;
					}
					keyNioBuffer = ((ReadableComponent) key).readableBuffer();
					assert keyNioBuffer.isDirect();
					assert keyNioBuffer.limit() == key.readableBytes();
				}
				try {
					return db.keyMayExist(cfh, readOptions, keyNioBuffer);
				} finally {
					if (mustCloseKey) {
						key.close();
					}
				}
			} else {
				byte[] keyBytes = LLUtils.toArray(key);
				return db.keyMayExist(cfh, readOptions, keyBytes, null);
			}
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void delete(WriteOptions writeOptions, Buffer key) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(writeOptions);
			keyBufferSize.record(key.readableBytes());
			if (nettyDirect) {
				// Get the key nio buffer to pass to RocksDB
				ByteBuffer keyNioBuffer;
				boolean mustCloseKey;
				{
					if (!LLUtils.isReadOnlyDirect(key)) {
						// If the nio buffer is not available, copy the netty buffer into a new direct buffer
						mustCloseKey = true;
						var directKey = DefaultBufferAllocators.offHeapAllocator().allocate(key.readableBytes());
						key.copyInto(key.readerOffset(), directKey, 0, key.readableBytes());
						key = directKey;
					} else {
						mustCloseKey = false;
					}
					keyNioBuffer = ((ReadableComponent) key).readableBuffer();
					assert keyNioBuffer.isDirect();
					assert keyNioBuffer.limit() == key.readableBytes();
				}
				try {
					db.delete(cfh, writeOptions, keyNioBuffer);
				} finally {
					if (mustCloseKey) {
						key.close();
					}
				}
			} else {
				db.delete(cfh, writeOptions, LLUtils.toArray(key));
			}
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void delete(WriteOptions writeOptions, byte[] key) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(writeOptions);
			keyBufferSize.record(key.length);
			db.delete(cfh, writeOptions, key);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public List<byte[]> multiGetAsList(ReadOptions readOptions, List<byte[]> keys) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			for (byte[] key : keys) {
				keyBufferSize.record(key.length);
			}
			var columnFamilyHandles = new RepeatedElementList<>(cfh, keys.size());
			return db.multiGetAsList(readOptions, columnFamilyHandles, keys);
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
	public void write(WriteOptions writeOptions, WriteBatch writeBatch) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(writeOptions);
			ensureOwned(writeBatch);
			db.write(writeOptions, writeBatch);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	/**
	 * @return true if committed successfully
	 */
	protected abstract boolean commitOptimistically(Transaction tx) throws RocksDBException;

	protected abstract Transaction beginTransaction(@NotNull WriteOptions writeOptions,
			TransactionOptions txOpts);

	@Override
	public final @NotNull UpdateAtomicResult updateAtomic(@NotNull ReadOptions readOptions,
			@NotNull WriteOptions writeOptions,
			Buffer key,
			BinarySerializationFunction updater,
			UpdateAtomicResultMode returnMode) throws IOException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			try {
				keyBufferSize.record(key.readableBytes());
				startedUpdate.increment();
				return updateAtomicImpl(readOptions, writeOptions, key, updater, returnMode);
			} catch (IOException e) {
				throw e;
			} catch (Exception e) {
				throw new IOException(e);
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

	protected abstract @NotNull UpdateAtomicResult updateAtomicImpl(@NotNull ReadOptions readOptions,
			@NotNull WriteOptions writeOptions,
			Buffer key,
			BinarySerializationFunction updater,
			UpdateAtomicResultMode returnMode) throws IOException;

	@Override
	@NotNull
	public RocksIteratorObj newIterator(@NotNull ReadOptions readOptions,
			@Nullable Buffer min,
			@Nullable Buffer max) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(readOptions);
			ensureOwned(min);
			ensureOwned(max);
			AbstractSlice<?> sliceMin;
			AbstractSlice<?> sliceMax;
			if (min != null) {
				sliceMin = setIterateBound(nettyDirect, readOptions, IterateBound.LOWER, min);
			} else {
				sliceMin = null;
			}
			try {
				if (max != null) {
					sliceMax = setIterateBound(nettyDirect, readOptions, IterateBound.UPPER, max);
				} else {
					sliceMax = null;
				}
				try {
					var it = db.newIterator(cfh, readOptions);
					try {
						return new RocksIteratorObj(it,
								sliceMin,
								sliceMax,
								min,
								max,
								nettyDirect,
								this.startedIterSeek,
								this.endedIterSeek,
								this.iterSeekTime,
								this.startedIterNext,
								this.endedIterNext,
								this.iterNextTime
						);
					} catch (Throwable ex) {
						it.close();
						throw ex;
					}
				} catch (Throwable ex) {
					if (sliceMax != null) {
						sliceMax.close();
					}
					throw ex;
				}
			} catch (Throwable ex) {
				if (sliceMin != null) {
					sliceMin.close();
				}
				throw ex;
			}
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	protected final Buffer applyUpdateAndCloseIfNecessary(BinarySerializationFunction updater,
			@Nullable Buffer prevDataToSendToUpdater)
			throws SerializationException {
		@Nullable Buffer newData = null;
		try {
			newData = updater.apply(prevDataToSendToUpdater);
		} finally {
			if (prevDataToSendToUpdater != newData && prevDataToSendToUpdater != null
					&& prevDataToSendToUpdater.isAccessible()) {
				prevDataToSendToUpdater.close();
			}
		}
		return newData;
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
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public ColumnFamilyHandle getColumnFamilyHandle() {
		return cfh;
	}

	@Override
	public BufferAllocator getAllocator() {
		return alloc;
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
}
