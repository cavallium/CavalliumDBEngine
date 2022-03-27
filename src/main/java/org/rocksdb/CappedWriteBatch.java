package org.rocksdb;

import static it.cavallium.dbengine.database.LLUtils.isDirect;
import static it.cavallium.dbengine.database.LLUtils.isReadOnlyDirect;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.ReadableComponent;
import io.netty5.buffer.api.Send;
import io.netty5.util.internal.PlatformDependent;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.RocksDBColumn;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CappedWriteBatch extends WriteBatch {

	/**
	 * Default: true, Use false to debug problems with direct buffers
	 */
	private static final boolean USE_FAST_DIRECT_BUFFERS = true;
	private final RocksDBColumn db;
	private final BufferAllocator alloc;
	private final int cap;
	private final WriteOptions writeOptions;

	private final List<Buffer> buffersToRelease;
	private final List<ByteBuffer> byteBuffersToRelease;

	/**
	 * @param db
	 * @param cap The limit of operations
	 */
	public CappedWriteBatch(RocksDBColumn db,
			BufferAllocator alloc,
			int cap,
			int reservedWriteBatchSize,
			long maxWriteBatchSize,
			WriteOptions writeOptions) {
		super(reservedWriteBatchSize);
		this.db = db;
		this.alloc = alloc;
		this.cap = cap;
		this.writeOptions = writeOptions;
		this.setMaxBytes(maxWriteBatchSize);
		this.buffersToRelease = new ArrayList<>();
		this.byteBuffersToRelease = new ArrayList<>();
	}

	private synchronized void flushIfNeeded(boolean force) throws RocksDBException {
		if (this.count() >= (force ? 1 : cap)) {
			try {
				db.write(writeOptions, this.getWriteBatch());
				this.clear();
			} finally {
				releaseAllBuffers();
			}
		}
	}

	private synchronized void releaseAllBuffers() {
		if (!buffersToRelease.isEmpty()) {
			for (Buffer byteBuffer : buffersToRelease) {
				byteBuffer.close();
			}
			buffersToRelease.clear();
		}
		if (!byteBuffersToRelease.isEmpty()) {
			for (var byteBuffer : byteBuffersToRelease) {
				PlatformDependent.freeDirectBuffer(byteBuffer);
			}
			byteBuffersToRelease.clear();
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

	public synchronized void put(ColumnFamilyHandle columnFamilyHandle,
			Send<Buffer> keyToReceive,
			Send<Buffer> valueToReceive) throws RocksDBException {
		var key = keyToReceive.receive();
		var value = valueToReceive.receive();
		if (USE_FAST_DIRECT_BUFFERS
				&& (isReadOnlyDirect(key))
				&& (isReadOnlyDirect(value))) {
			ByteBuffer keyNioBuffer = ((ReadableComponent) key).readableBuffer();
			ByteBuffer valueNioBuffer = ((ReadableComponent) value).readableBuffer();
			buffersToRelease.add(value);
			buffersToRelease.add(key);

			super.put(columnFamilyHandle, keyNioBuffer, valueNioBuffer);
		} else {
			try {
				byte[] keyArray = LLUtils.toArray(key);
				byte[] valueArray = LLUtils.toArray(value);
				super.put(columnFamilyHandle, keyArray, valueArray);
			} finally {
				key.close();
				value.close();
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

	public synchronized void delete(ColumnFamilyHandle columnFamilyHandle, Send<Buffer> keyToReceive) throws RocksDBException {
		var key = keyToReceive.receive();
		if (USE_FAST_DIRECT_BUFFERS && isReadOnlyDirect(key)) {
			ByteBuffer keyNioBuffer = ((ReadableComponent) key).readableBuffer();
			buffersToRelease.add(key);
			delete(columnFamilyHandle, keyNioBuffer);
		} else {
			try {
				super.delete(columnFamilyHandle, LLUtils.toArray(key));
			} finally {
				key.close();
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
	public synchronized void delete(ByteBuffer key) throws RocksDBException {
		super.delete(key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void delete(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key) throws RocksDBException {
		super.delete(columnFamilyHandle, key);
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
