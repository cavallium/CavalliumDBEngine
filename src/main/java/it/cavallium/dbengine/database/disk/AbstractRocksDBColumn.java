package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.MemoryManager;
import io.net5.buffer.api.Send;
import io.net5.util.internal.PlatformDependent;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.LLUtils.DirectBuffer;
import it.cavallium.dbengine.database.RepeatedElementList;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.lucene.ExponentialPageLimits;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Holder;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public sealed abstract class AbstractRocksDBColumn<T extends RocksDB> implements RocksDBColumn
		permits StandardRocksDBColumn, OptimisticRocksDBColumn, PessimisticRocksDBColumn {

	private static final int INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES = 4096;
	private static final byte[] NO_DATA = new byte[0];
	protected static final UpdateAtomicResult RESULT_NOTHING = new UpdateAtomicResultNothing();

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final T db;
	private final DatabaseOptions opts;
	private final BufferAllocator alloc;
	private final ColumnFamilyHandle cfh;

	public AbstractRocksDBColumn(T db, DatabaseOptions databaseOptions, BufferAllocator alloc, ColumnFamilyHandle cfh) {
		this.db = db;
		this.opts = databaseOptions;
		this.alloc = alloc;
		this.cfh = cfh;
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
	public @Nullable Send<Buffer> get(@NotNull ReadOptions readOptions,
			Send<Buffer> keySend,
			boolean existsAlmostCertainly) throws RocksDBException {
		try (var key = keySend.receive()) {
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called dbGet in a nonblocking thread");
			}
			if (opts.allowNettyDirect()) {

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
						// todo: use keyMayExist when rocksdb will implement keyMayExist with buffers
						valueSize = db.get(cfh,
								readOptions,
								keyNioBuffer.byteBuffer().position(0),
								resultNioBuf
						);
						if (valueSize != RocksDB.NOT_FOUND) {

							// todo: check if position is equal to data that have been read
							// todo: check if limit is equal to value size or data that have been read
							assert valueSize <= 0 || resultNioBuf.limit() > 0;

							// Check if read data is not bigger than the total value size.
							// If it's bigger it means that RocksDB is writing the start
							// of the result into the result buffer more than once.
							assert resultNioBuf.limit() <= valueSize;

							if (valueSize <= resultNioBuf.limit()) {
								// Return the result ready to be read
								return resultBuf.readerOffset(0).writerOffset(valueSize).send();
							} else {
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
				try {
					byte[] keyArray = LLUtils.toArray(key);
					requireNonNull(keyArray);
					Holder<byte[]> data = existsAlmostCertainly ? null : new Holder<>();
					if (existsAlmostCertainly || db.keyMayExist(cfh, readOptions, keyArray, data)) {
						if (!existsAlmostCertainly && data.getValue() != null) {
							return LLUtils.fromByteArray(alloc, data.getValue()).send();
						} else {
							byte[] result = db.get(cfh, readOptions, keyArray);
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
					if (!(readOptions instanceof UnreleasableReadOptions)) {
						readOptions.close();
					}
				}
			}
		}
	}

	@Override
	public void put(@NotNull WriteOptions writeOptions, Send<Buffer> keyToReceive,
			Send<Buffer> valueToReceive) throws RocksDBException {
		try {
			try (var key = keyToReceive.receive()) {
				try (var value = valueToReceive.receive()) {
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called dbPut in a nonblocking thread");
					}
					assert key.isAccessible();
					assert value.isAccessible();
					if (opts.allowNettyDirect()) {
						var keyNioBuffer = LLUtils.convertToReadableDirect(alloc, key.send());
						try (var ignored1 = keyNioBuffer.buffer().receive()) {
							assert keyNioBuffer.byteBuffer().isDirect();
							var valueNioBuffer = LLUtils.convertToReadableDirect(alloc, value.send());
							try (var ignored2 = valueNioBuffer.buffer().receive()) {
								assert valueNioBuffer.byteBuffer().isDirect();
								db.put(cfh, writeOptions, keyNioBuffer.byteBuffer(), valueNioBuffer.byteBuffer());
							} finally {
								PlatformDependent.freeDirectBuffer(valueNioBuffer.byteBuffer());
							}
						} finally {
							PlatformDependent.freeDirectBuffer(keyNioBuffer.byteBuffer());
						}
					} else {
						db.put(cfh, writeOptions, LLUtils.toArray(key), LLUtils.toArray(value));
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
	public boolean exists(@NotNull ReadOptions readOptions, Send<Buffer> keySend) throws RocksDBException {
		try (var key = keySend.receive()) {
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called containsKey in a nonblocking thread");
			}
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
				if (readOptions != null && !(readOptions instanceof UnreleasableReadOptions)) {
					readOptions.close();
				}
			}
			return size != RocksDB.NOT_FOUND;
		}
	}

	@Override
	public void delete(WriteOptions writeOptions, Send<Buffer> keySend) throws RocksDBException {
		try (var key = keySend.receive()) {
			if (opts.allowNettyDirect()) {
				DirectBuffer keyNioBuffer = LLUtils.convertToReadableDirect(alloc, key.send());
				try {
					db.delete(cfh, writeOptions, keyNioBuffer.byteBuffer());
				} finally {
					keyNioBuffer.buffer().close();
					PlatformDependent.freeDirectBuffer(keyNioBuffer.byteBuffer());
				}
			} else {
				db.delete(cfh, writeOptions, LLUtils.toArray(key));
			}
		}
	}

	@Override
	public void delete(WriteOptions writeOptions, byte[] key) throws RocksDBException {
		db.delete(cfh, writeOptions, key);
	}

	@Override
	public List<byte[]> multiGetAsList(ReadOptions readOptions, List<byte[]> keys) throws RocksDBException {
		var columnFamilyHandles = new RepeatedElementList<>(cfh, keys.size());
		return db.multiGetAsList(readOptions, columnFamilyHandles, keys);
	}

	@Override
	public void suggestCompactRange() throws RocksDBException {
		db.suggestCompactRange(cfh);
	}

	@Override
	public void compactRange(byte[] begin, byte[] end, CompactRangeOptions options)
			throws RocksDBException {
		db.compactRange(cfh, begin, end, options);
	}

	@Override
	public void flush(FlushOptions options) throws RocksDBException {
		db.flush(options, cfh);
	}

	@Override
	public void flushWal(boolean sync) throws RocksDBException {
		db.flushWal(sync);
	}

	@Override
	public long getLongProperty(String property) throws RocksDBException {
		return db.getLongProperty(cfh, property);
	}

	@Override
	public void write(WriteOptions writeOptions, WriteBatch writeBatch) throws RocksDBException {
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
}
