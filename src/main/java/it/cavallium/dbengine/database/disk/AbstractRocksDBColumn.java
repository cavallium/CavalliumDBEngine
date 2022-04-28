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
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLLocalDictionary.ReleasableSliceImplWithRelease;
import it.cavallium.dbengine.database.disk.LLLocalDictionary.ReleasableSliceImplWithoutRelease;
import it.cavallium.dbengine.database.serialization.SerializationException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractSlice;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.CompactionOptions;
import org.rocksdb.DirectSlice;
import org.rocksdb.FlushOptions;
import org.rocksdb.Holder;
import org.rocksdb.KeyMayExist.KeyMayExistEnum;
import org.rocksdb.LevelMetaData;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.rocksdb.SstFileMetaData;
import org.rocksdb.Transaction;
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
	protected final Lock accessibilityLock;
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
			Lock accessibilityLock) {
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
		this.accessibilityLock = accessibilityLock;

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
	static ReleasableSlice setIterateBound(boolean allowNettyDirect,
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
		return new ReleasableSliceImplWithRelease(slice);
	}

	static ReleasableSlice emptyReleasableSlice() {
		var arr = new byte[0];

		return new ReleasableSliceImplWithoutRelease(new Slice(arr));
	}

	/**
	 * This method should not modify or move the writerIndex/readerIndex of the buffers inside the range
	 */
	@NotNull
	public RocksIteratorTuple getRocksIterator(boolean allowNettyDirect,
			ReadOptions readOptions,
			LLRange range,
			boolean reverse) throws RocksDBException {
		assert !Schedulers.isInNonBlockingThread() : "Called getRocksIterator in a nonblocking thread";
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
		var rocksIterator = this.newIterator(readOptions);
		SafeCloseable seekFromOrTo;
		if (reverse) {
			if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMax()) {
				seekFromOrTo = Objects.requireNonNullElseGet(rocksIterator.seekFrom(range.getMaxUnsafe()),
						() -> ((SafeCloseable) () -> {}));
			} else {
				seekFromOrTo = () -> {};
				rocksIterator.seekToLast();
			}
		} else {
			if (!LLLocalDictionary.PREFER_AUTO_SEEK_BOUND && range.hasMin()) {
				seekFromOrTo = Objects.requireNonNullElseGet(rocksIterator.seekTo(range.getMinUnsafe()),
						() -> ((SafeCloseable) () -> {}));
			} else {
				seekFromOrTo = () -> {};
				rocksIterator.seekToFirst();
			}
		}
		return new RocksIteratorTuple(List.of(readOptions), rocksIterator, sliceMin, sliceMax, seekFromOrTo);
	}

	protected T getDb() {
		return db;
	}

	protected ColumnFamilyHandle getCfh() {
		return cfh;
	}

	@Override
	public @Nullable Buffer get(@NotNull ReadOptions readOptions, Buffer key)
			throws RocksDBException {
		if (Schedulers.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called dbGet in a nonblocking thread");
		}
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!readOptions.isOwningHandle()) {
			throw new IllegalStateException("ReadOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
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

						var keyMayExist = db.keyMayExist(cfh, readOptions, keyNioBuffer.rewind(), resultWritable.clear());
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
				try {
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
				} finally {
					if (!(readOptions instanceof UnreleasableReadOptions)) {
						readOptions.close();
					}
				}
			}
		} finally {
			readAttempts.record(readAttemptsCount);
		}
	}

	@Override
	public void put(@NotNull WriteOptions writeOptions, Buffer key, Buffer value) throws RocksDBException {
		try {
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called dbPut in a nonblocking thread");
			}
			if (!db.isOwningHandle()) {
				throw new IllegalStateException("Database is closed");
			}
			if (!writeOptions.isOwningHandle()) {
				throw new IllegalStateException("WriteOptions is closed");
			}
			if (!cfh.isOwningHandle()) {
				throw new IllegalStateException("Column family is closed");
			}
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
			if (!(writeOptions instanceof UnreleasableWriteOptions)) {
				writeOptions.close();
			}
		}
	}

	@Override
	public boolean exists(@NotNull ReadOptions readOptions, Buffer key) throws RocksDBException {
		if (Schedulers.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called containsKey in a nonblocking thread");
		}
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!readOptions.isOwningHandle()) {
			throw new IllegalStateException("ReadOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
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
			try {
				if (db.keyMayExist(cfh, readOptions, keyBytes, data)) {
					mayExistHit = true;
					if (data.getValue() != null) {
						size = data.getValue().length;
					} else {
						size = db.get(cfh, readOptions, keyBytes, NO_DATA);
					}
				}
			} finally {
				if (!(readOptions instanceof UnreleasableReadOptions)) {
					readOptions.close();
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
	}

	@Override
	public boolean mayExists(@NotNull ReadOptions readOptions, Buffer key) throws RocksDBException {
		if (Schedulers.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called containsKey in a nonblocking thread");
		}
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!readOptions.isOwningHandle()) {
			throw new IllegalStateException("ReadOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
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
				return db.keyMayExist(cfh, keyNioBuffer);
			} finally {
				if (mustCloseKey) {
					key.close();
				}
			}
		} else {
			byte[] keyBytes = LLUtils.toArray(key);
			try {
				return db.keyMayExist(cfh, readOptions, keyBytes, null);
			} finally {
				if (!(readOptions instanceof UnreleasableReadOptions)) {
					readOptions.close();
				}
			}
		}
	}

	@Override
	public void delete(WriteOptions writeOptions, Buffer key) throws RocksDBException {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!writeOptions.isOwningHandle()) {
			throw new IllegalStateException("WriteOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
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
	}

	@Override
	public void delete(WriteOptions writeOptions, byte[] key) throws RocksDBException {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!writeOptions.isOwningHandle()) {
			throw new IllegalStateException("WriteOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
		keyBufferSize.record(key.length);
		db.delete(cfh, writeOptions, key);
	}

	@Override
	public List<byte[]> multiGetAsList(ReadOptions readOptions, List<byte[]> keys) throws RocksDBException {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!readOptions.isOwningHandle()) {
			throw new IllegalStateException("ReadOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
		for (byte[] key : keys) {
			keyBufferSize.record(key.length);
		}
		var columnFamilyHandles = new RepeatedElementList<>(cfh, keys.size());
		return db.multiGetAsList(readOptions, columnFamilyHandles, keys);
	}

	@Override
	public void suggestCompactRange() throws RocksDBException {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
		db.suggestCompactRange(cfh);
	}

	@Override
	public void compactRange(byte[] begin, byte[] end, CompactRangeOptions options)
			throws RocksDBException {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!options.isOwningHandle()) {
			throw new IllegalStateException("CompactRangeOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
		db.compactRange(cfh, begin, end, options);
	}

	@Override
	public void flush(FlushOptions options) throws RocksDBException {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!options.isOwningHandle()) {
			throw new IllegalStateException("FlushOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
		db.flush(options, cfh);
	}

	@Override
	public void flushWal(boolean sync) throws RocksDBException {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
		db.flushWal(sync);
	}

	@Override
	public long getLongProperty(String property) throws RocksDBException {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
		return db.getLongProperty(cfh, property);
	}

	@Override
	public void write(WriteOptions writeOptions, WriteBatch writeBatch) throws RocksDBException {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!writeOptions.isOwningHandle()) {
			throw new IllegalStateException("WriteOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
		db.write(writeOptions, writeBatch);
	}

	/**
	 * @return true if committed successfully
	 */
	protected abstract boolean commitOptimistically(Transaction tx) throws RocksDBException;

	protected abstract Transaction beginTransaction(@NotNull WriteOptions writeOptions);

	@Override
	public final @NotNull UpdateAtomicResult updateAtomic(@NotNull ReadOptions readOptions,
			@NotNull WriteOptions writeOptions,
			Buffer key,
			BinarySerializationFunction updater,
			UpdateAtomicResultMode returnMode) throws IOException {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!readOptions.isOwningHandle()) {
			throw new IllegalStateException("ReadOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
		try {
			keyBufferSize.record(key.readableBytes());
			startedUpdate.increment();
			accessibilityLock.lock();
			return updateAtomicImpl(readOptions, writeOptions, key, updater, returnMode);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			accessibilityLock.unlock();
			endedUpdate.increment();
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
	public RocksDBIterator newIterator(@NotNull ReadOptions readOptions) {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!readOptions.isOwningHandle()) {
			throw new IllegalStateException("ReadOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
		return new RocksDBIterator(db.newIterator(cfh, readOptions),
				nettyDirect,
				this.startedIterSeek,
				this.endedIterSeek,
				this.iterSeekTime,
				this.startedIterNext,
				this.endedIterNext,
				this.iterNextTime
		);
	}

	protected final Buffer applyUpdateAndCloseIfNecessary(BinarySerializationFunction updater,
			@Nullable Buffer prevDataToSendToUpdater)
			throws SerializationException {
		@Nullable Buffer newData = null;
		try {
			newData = updater.apply(prevDataToSendToUpdater);
		} finally {
			if (prevDataToSendToUpdater != newData && prevDataToSendToUpdater != null) {
				prevDataToSendToUpdater.close();
			}
		}
		return newData;
	}

	@Override
	public final void forceCompaction(int volumeId) throws RocksDBException {
		List<String> files = new ArrayList<>();
		var meta = db.getColumnFamilyMetaData(cfh);
		int bottommostLevel = -1;
		for (LevelMetaData level : meta.levels()) {
			bottommostLevel = Math.max(bottommostLevel, level.level());
		}
		int count = 0;
		x: for (LevelMetaData level : meta.levels()) {
			for (SstFileMetaData file : level.files()) {
				if (file.fileName().endsWith(".sst")) {
					files.add(file.fileName());
					count++;
					if (count >= 4) {
						break x;
					}
				}
			}
		}
		try (var co = new CompactionOptions()) {
			if (!files.isEmpty() && bottommostLevel != -1) {
				db.compactFiles(co, cfh, files, bottommostLevel, volumeId, null);
			}
			db.compactRange(cfh);
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
