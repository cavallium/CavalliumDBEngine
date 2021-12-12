package it.cavallium.dbengine.database.disk;

import static io.net5.buffer.api.StandardAllocationTypes.OFF_HEAP;
import static it.cavallium.dbengine.database.LLUtils.INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES;
import static java.util.Objects.requireNonNull;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.AllocationType;
import io.net5.buffer.api.AllocatorControl;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.CompositeBuffer;
import io.net5.buffer.api.MemoryManager;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.StandardAllocationTypes;
import io.net5.buffer.api.WritableComponent;
import io.net5.buffer.api.internal.Statics;
import io.net5.buffer.api.unsafe.UnsafeMemoryManager;
import io.net5.util.internal.PlatformDependent;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.LLUtils.DirectBuffer;
import it.cavallium.dbengine.database.RepeatedElementList;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.FileOperationInfo;
import org.rocksdb.FlushOptions;
import org.rocksdb.Holder;
import org.rocksdb.KeyMayExistWorkaround;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.scheduler.Schedulers;
import sun.misc.Unsafe;

public sealed abstract class AbstractRocksDBColumn<T extends RocksDB> implements RocksDBColumn
		permits StandardRocksDBColumn, OptimisticRocksDBColumn, PessimisticRocksDBColumn {

	private static final byte[] NO_DATA = new byte[0];
	protected static final UpdateAtomicResult RESULT_NOTHING = new UpdateAtomicResultNothing();

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final T db;
	private final DatabaseOptions opts;
	private final boolean nettyDirect;
	private final BufferAllocator alloc;
	private final ColumnFamilyHandle cfh;

	private final MeterRegistry meterRegistry;
	private final AtomicInteger lastDataSizeMetric = new AtomicInteger(0);

	public AbstractRocksDBColumn(T db,
			DatabaseOptions databaseOptions,
			BufferAllocator alloc,
			ColumnFamilyHandle cfh,
			MeterRegistry meterRegistry) {
		this.db = db;
		this.opts = databaseOptions;
		this.nettyDirect = opts.allowNettyDirect() && alloc.getAllocationType() == OFF_HEAP;
		this.alloc = alloc;
		this.cfh = cfh;

		this.meterRegistry = meterRegistry;
		Gauge
				.builder("it.cavallium.dbengine.database.disk.column.lastdatasize", lastDataSizeMetric::get)
				.description("Last data size read using get()")
				.register(meterRegistry);
	}

	protected T getDb() {
		return db;
	}

	protected DatabaseOptions getOpts() {
		return opts;
	}

	protected ColumnFamilyHandle getCfh() {
		return cfh;
	}

	@Override
	public @Nullable Buffer get(@NotNull ReadOptions readOptions, Buffer key, boolean existsAlmostCertainly)
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
		if (nettyDirect) {
			// Get the key nio buffer to pass to RocksDB
			ByteBuffer keyNioBuffer = LLUtils.asReadOnlyDirect(key);
			assert keyNioBuffer.isDirect();
			boolean mustCloseKey;
			if (keyNioBuffer == null) {
				mustCloseKey = true;
				// If the nio buffer is not available, copy the netty buffer into a new direct buffer
				keyNioBuffer = LLUtils.copyToNewDirectBuffer(key);
			} else {
				mustCloseKey = false;
			}
			assert keyNioBuffer.limit() == key.readableBytes();
			try {
				// Create a direct result buffer because RocksDB works only with direct buffers
				var resultBuffer = LLUtils.allocateShared(INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES);
				try {
					assert resultBuffer.readerOffset() == 0;
					assert resultBuffer.writerOffset() == 0;
					var resultWritable = ((WritableComponent) resultBuffer).writableBuffer();

					var keyMayExist = db.keyMayExist(cfh, keyNioBuffer, resultWritable);
					var keyMayExistState = KeyMayExistWorkaround.getExistenceState(keyMayExist);
					int keyMayExistValueLength = KeyMayExistWorkaround.getValueLength(keyMayExist);
					// At the beginning, size reflects the expected size, then it becomes the real data size
					int size = keyMayExistState == 2 ? keyMayExistValueLength : -1;
					switch (keyMayExistState) {
						// kNotExist
						case 0: {
							resultBuffer.close();
							return null;
						}
						// kExistsWithoutValue
						case 1: {
							assert keyMayExistValueLength == 0;
							resultWritable.clear();
							// real data size
							size = db.get(cfh, readOptions, keyNioBuffer, resultWritable);
							if (size == RocksDB.NOT_FOUND) {
								resultBuffer.close();
								return null;
							}
						}
						// kExistsWithValue
						case 2: {
							// real data size
							this.lastDataSizeMetric.set(size);
							assert size >= 0;
							if (size <= resultWritable.limit()) {
								assert size == resultWritable.limit();
								return resultBuffer.writerOffset(resultWritable.limit());
							} else {
								resultBuffer.ensureWritable(size);
								resultWritable = ((WritableComponent) resultBuffer).writableBuffer();
								assert resultBuffer.readerOffset() == 0;
								assert resultBuffer.writerOffset() == 0;

								size = db.get(cfh, readOptions, keyNioBuffer, resultWritable);
								if (size == RocksDB.NOT_FOUND) {
									resultBuffer.close();
									return null;
								}
								assert size == resultWritable.limit();
								return resultBuffer.writerOffset(resultWritable.limit());
							}
						}
						default: {
							throw new IllegalStateException();
						}
					}
				} catch (Throwable t) {
					resultBuffer.close();
					throw t;
				}
			} finally {
				if (mustCloseKey) {
					PlatformDependent.freeDirectBuffer(keyNioBuffer);
				}
			}
		} else {
			try {
				byte[] keyArray = LLUtils.toArray(key);
				requireNonNull(keyArray);
				Holder<byte[]> data = existsAlmostCertainly ? null : new Holder<>();
				if (existsAlmostCertainly || db.keyMayExist(cfh, readOptions, keyArray, data)) {
					if (!existsAlmostCertainly && data.getValue() != null) {
						return LLUtils.fromByteArray(alloc, data.getValue());
					} else {
						byte[] result = db.get(cfh, readOptions, keyArray);
						if (result == null) {
							return null;
						} else {
							return LLUtils.fromByteArray(alloc, result);
						}
					}
				} else {
					return null;
				}
			} finally {
				if (!(readOptions instanceof UnreleasableReadOptions)) {
					readOptions.close();
				}
			}
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
			if (nettyDirect) {
				// Get the key nio buffer to pass to RocksDB
				ByteBuffer keyNioBuffer = LLUtils.asReadOnlyDirect(key);
				boolean mustCloseKey;
				if (keyNioBuffer == null) {
					mustCloseKey = true;
					// If the nio buffer is not available, copy the netty buffer into a new direct buffer
					keyNioBuffer = LLUtils.copyToNewDirectBuffer(key);
				} else {
					mustCloseKey = false;
				}
				try {
					// Get the value nio buffer to pass to RocksDB
					ByteBuffer valueNioBuffer = LLUtils.asReadOnlyDirect(value);
					boolean mustCloseValue;
					if (valueNioBuffer == null) {
						mustCloseValue = true;
						// If the nio buffer is not available, copy the netty buffer into a new direct buffer
						valueNioBuffer = LLUtils.copyToNewDirectBuffer(value);
					} else {
						mustCloseValue = false;
					}
					try {
						db.put(cfh, writeOptions, keyNioBuffer, valueNioBuffer);
					} finally {
						if (mustCloseValue) {
							PlatformDependent.freeDirectBuffer(valueNioBuffer);
						}
					}
				} finally {
					if (mustCloseKey) {
						PlatformDependent.freeDirectBuffer(keyNioBuffer);
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
			ByteBuffer keyNioBuffer = LLUtils.asReadOnlyDirect(key);
			boolean mustCloseKey;
			if (keyNioBuffer == null) {
				mustCloseKey = true;
				// If the nio buffer is not available, copy the netty buffer into a new direct buffer
				keyNioBuffer = LLUtils.copyToNewDirectBuffer(key);
			} else {
				mustCloseKey = false;
			}
			try {
				if (db.keyMayExist(cfh, keyNioBuffer)) {
					int size = db.get(cfh, readOptions, keyNioBuffer, LLUtils.EMPTY_BYTE_BUFFER);
					return size != RocksDB.NOT_FOUND;
				} else {
					return false;
				}
			} finally {
				if (mustCloseKey) {
					PlatformDependent.freeDirectBuffer(keyNioBuffer);
				}
			}
		} else {
			int size = RocksDB.NOT_FOUND;
			byte[] keyBytes = LLUtils.toArray(key);
			Holder<byte[]> data = new Holder<>();
			try {
				if (db.keyMayExist(cfh, readOptions, keyBytes, data)) {
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
			return size != RocksDB.NOT_FOUND;
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
		if (nettyDirect) {
			// Get the key nio buffer to pass to RocksDB
			ByteBuffer keyNioBuffer = LLUtils.asReadOnlyDirect(key);
			boolean mustCloseKey;
			if (keyNioBuffer == null) {
				mustCloseKey = true;
				// If the nio buffer is not available, copy the netty buffer into a new direct buffer
				keyNioBuffer = LLUtils.copyToNewDirectBuffer(key);
			} else {
				mustCloseKey = false;
			}
			try {
				db.delete(cfh, writeOptions, keyNioBuffer);
			} finally {
				if (mustCloseKey) {
					PlatformDependent.freeDirectBuffer(keyNioBuffer);
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
	@NotNull
	public RocksIterator newIterator(@NotNull ReadOptions readOptions) {
		if (!db.isOwningHandle()) {
			throw new IllegalStateException("Database is closed");
		}
		if (!readOptions.isOwningHandle()) {
			throw new IllegalStateException("ReadOptions is closed");
		}
		if (!cfh.isOwningHandle()) {
			throw new IllegalStateException("Column family is closed");
		}
		return db.newIterator(cfh, readOptions);
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
}
