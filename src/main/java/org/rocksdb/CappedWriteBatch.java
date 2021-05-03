package org.rocksdb;

import io.netty.buffer.ByteBuf;
import it.cavallium.dbengine.database.LLUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.rocksdb.AbstractWriteBatch;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchInterface;
import org.rocksdb.WriteOptions;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;

@NotAtomic
public class CappedWriteBatch extends WriteBatch {

	/**
	 * Default: true, Use false to debug problems with direct buffers
	 */
	private static final boolean USE_FAST_DIRECT_BUFFERS = true;
	private final RocksDB db;
	private final int cap;
	private final WriteOptions writeOptions;
	
	private final List<ByteBuf> buffersToRelease;

	/**
	 * @param cap The limit of operations
	 */
	public CappedWriteBatch(RocksDB db,
			int cap,
			int reservedWriteBatchSize,
			long maxWriteBatchSize,
			WriteOptions writeOptions) {
		super(reservedWriteBatchSize);
		this.db = db;
		this.cap = cap;
		this.writeOptions = writeOptions;
		this.setMaxBytes(maxWriteBatchSize);
		this.buffersToRelease = new ArrayList<>();
	}

	private synchronized void flushIfNeeded(boolean force) throws RocksDBException {
		if (this.count() >= (force ? 1 : cap)) {
			db.write(writeOptions, this.getWriteBatch());
			this.clear();
			releaseAllBuffers();
		}
	}

	private synchronized void releaseAllBuffers() {
		if (!buffersToRelease.isEmpty()) {
			for (ByteBuf byteBuffer : buffersToRelease) {
				assert byteBuffer.refCnt() > 0;
				byteBuffer.release();
			}
			buffersToRelease.clear();
		}
	}

	@Override
	public synchronized int count() {
		return super.count();
	}

	@Override
	public synchronized void put(byte[] key, byte[] value) throws RocksDBException {
		super.put(key, value);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
		super.put(columnFamilyHandle, key, value);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void put(ByteBuffer key, ByteBuffer value) throws RocksDBException {
		super.put(key, value);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value) throws RocksDBException {
		super.put(columnFamilyHandle, key, value);
		flushIfNeeded(false);
	}

	public synchronized void put(ColumnFamilyHandle columnFamilyHandle, ByteBuf key, ByteBuf value) throws RocksDBException {
		if (USE_FAST_DIRECT_BUFFERS && key.isDirect() && value.isDirect()) {
			buffersToRelease.add(key);
			buffersToRelease.add(value);
			ByteBuffer keyNioBuffer = LLUtils.toDirect(key);
			assert keyNioBuffer.isDirect();

			ByteBuffer valueNioBuffer = LLUtils.toDirect(value);
			assert valueNioBuffer.isDirect();
			super.put(columnFamilyHandle, keyNioBuffer, valueNioBuffer);
		} else {
			try {
				byte[] keyArray = LLUtils.toArray(key);
				byte[] valueArray = LLUtils.toArray(value);
				super.put(columnFamilyHandle, keyArray, valueArray);
			} finally {
				key.release();
				value.release();
			}
		}
		flushIfNeeded(false);
	}

	@Override
	public synchronized void merge(byte[] key, byte[] value) throws RocksDBException {
		super.merge(key, value);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
		super.merge(columnFamilyHandle, key, value);
		flushIfNeeded(false);
	}

	@Deprecated
	@Override
	public synchronized void remove(byte[] key) throws RocksDBException {
		super.remove(key);
		flushIfNeeded(false);
	}

	@Deprecated
	@Override
	public synchronized void remove(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
		super.remove(columnFamilyHandle, key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void delete(byte[] key) throws RocksDBException {
		super.delete(key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
		super.delete(columnFamilyHandle, key);
		flushIfNeeded(false);
	}

	public synchronized void delete(ColumnFamilyHandle columnFamilyHandle, ByteBuf key) throws RocksDBException {
		if (USE_FAST_DIRECT_BUFFERS) {
			buffersToRelease.add(key);
			ByteBuffer keyNioBuffer = LLUtils.toDirect(key);
			assert keyNioBuffer.isDirect();
			removeDirect(nativeHandle_,
					keyNioBuffer,
					keyNioBuffer.position(),
					keyNioBuffer.remaining(),
					columnFamilyHandle.nativeHandle_
			);
			keyNioBuffer.position(keyNioBuffer.limit());
		} else {
			try {
				super.delete(columnFamilyHandle, LLUtils.toArray(key));
			} finally {
				key.release();
			}
		}
		flushIfNeeded(false);
	}

	@Override
	public synchronized void singleDelete(byte[] key) throws RocksDBException {
		super.singleDelete(key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void singleDelete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
		super.singleDelete(columnFamilyHandle, key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void remove(ByteBuffer key) throws RocksDBException {
		super.remove(key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void remove(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key) throws RocksDBException {
		super.remove(columnFamilyHandle, key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void deleteRange(byte[] beginKey, byte[] endKey) throws RocksDBException {
		super.deleteRange(beginKey, endKey);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey)
			throws RocksDBException {
		super.deleteRange(columnFamilyHandle, beginKey, endKey);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void putLogData(byte[] blob) throws RocksDBException {
		super.putLogData(blob);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void clear() {
		super.clear();
		releaseAllBuffers();
	}

	@Override
	public synchronized void setSavePoint() {
		super.setSavePoint();
	}

	@Override
	public synchronized void rollbackToSavePoint() throws RocksDBException {
		super.rollbackToSavePoint();
	}

	@Override
	public synchronized void popSavePoint() throws RocksDBException {
		super.popSavePoint();
	}

	@Override
	public synchronized void setMaxBytes(long maxBytes) {
		super.setMaxBytes(maxBytes);
	}

	@Override
	public synchronized WriteBatch getWriteBatch() {
		return super.getWriteBatch();
	}

	public synchronized void writeToDbAndClose() throws RocksDBException {
		try {
			flushIfNeeded(true);
			super.close();
		} finally {
			releaseAllBuffers();
		}
	}

	public void flush() throws RocksDBException {
		try {
			flushIfNeeded(true);
		} finally {
			releaseAllBuffers();
		}
	}

	@Override
	public synchronized void close() {
		super.close();
		releaseAllBuffers();
	}
}
