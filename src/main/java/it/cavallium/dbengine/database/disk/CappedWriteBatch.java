package it.cavallium.dbengine.database.disk;

import java.nio.ByteBuffer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchInterface;
import org.rocksdb.WriteOptions;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;

@NotAtomic
public class CappedWriteBatch implements WriteBatchInterface, AutoCloseable {

	private final RocksDB db;
	private final int cap;
	private final WriteOptions writeOptions;

	private final WriteBatch writeBatch;

	/**
	 * @param cap The limit of operations
	 */
	public CappedWriteBatch(RocksDB db,
			int cap,
			int reservedWriteBatchSize,
			long maxWriteBatchSize,
			WriteOptions writeOptions) {
		this.db = db;
		this.cap = cap;
		this.writeOptions = writeOptions;
		this.writeBatch = new WriteBatch(reservedWriteBatchSize);
		this.writeBatch.setMaxBytes(maxWriteBatchSize);
	}

	private synchronized void flushIfNeeded(boolean force) throws RocksDBException {
		if (this.writeBatch.count() >= (force ? 1 : cap)) {
			db.write(writeOptions, this.writeBatch);
			this.writeBatch.clear();
		}
	}

	@Override
	public synchronized int count() {
		return writeBatch.count();
	}

	@Override
	public synchronized void put(byte[] key, byte[] value) throws RocksDBException {
		writeBatch.put(key, value);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
		writeBatch.put(columnFamilyHandle, key, value);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void put(ByteBuffer key, ByteBuffer value) throws RocksDBException {
		writeBatch.put(key, value);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value) throws RocksDBException {
		writeBatch.put(columnFamilyHandle, key, value);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void merge(byte[] key, byte[] value) throws RocksDBException {
		writeBatch.merge(key, value);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
		writeBatch.merge(columnFamilyHandle, key, value);
		flushIfNeeded(false);
	}

	@Deprecated
	@Override
	public synchronized void remove(byte[] key) throws RocksDBException {
		writeBatch.remove(key);
		flushIfNeeded(false);
	}

	@Deprecated
	@Override
	public synchronized void remove(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
		writeBatch.remove(columnFamilyHandle, key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void delete(byte[] key) throws RocksDBException {
		writeBatch.delete(key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
		writeBatch.delete(columnFamilyHandle, key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void singleDelete(byte[] key) throws RocksDBException {
		writeBatch.singleDelete(key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void singleDelete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
		writeBatch.singleDelete(columnFamilyHandle, key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void remove(ByteBuffer key) throws RocksDBException {
		writeBatch.remove(key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void remove(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key) throws RocksDBException {
		writeBatch.remove(columnFamilyHandle, key);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void deleteRange(byte[] beginKey, byte[] endKey) throws RocksDBException {
		writeBatch.deleteRange(beginKey, endKey);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey)
			throws RocksDBException {
		writeBatch.deleteRange(columnFamilyHandle, beginKey, endKey);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void putLogData(byte[] blob) throws RocksDBException {
		writeBatch.putLogData(blob);
		flushIfNeeded(false);
	}

	@Override
	public synchronized void clear() {
		writeBatch.clear();
	}

	@Override
	public synchronized void setSavePoint() {
		writeBatch.setSavePoint();
	}

	@Override
	public synchronized void rollbackToSavePoint() throws RocksDBException {
		writeBatch.rollbackToSavePoint();
	}

	@Override
	public synchronized void popSavePoint() throws RocksDBException {
		writeBatch.popSavePoint();
	}

	@Override
	public synchronized void setMaxBytes(long maxBytes) {
		writeBatch.setMaxBytes(maxBytes);
	}

	@Override
	public synchronized WriteBatch getWriteBatch() {
		return writeBatch;
	}

	public synchronized void writeToDbAndClose() throws RocksDBException {
		flushIfNeeded(true);
	}

	@Override
	public synchronized void close() {
		writeBatch.close();
	}
}
