package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.asArray;

import it.cavallium.dbengine.buffers.Buf;
import java.nio.ByteBuffer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class CappedWriteBatch extends WriteBatch {

	private final RocksDBColumn db;
	private final int cap;
	private final WriteOptions writeOptions;

	/**
	 * @param db
	 * @param cap The limit of operations
	 */
	public CappedWriteBatch(RocksDBColumn db,
			int cap,
			int reservedWriteBatchSize,
			long maxWriteBatchSize,
			WriteOptions writeOptions) {
		super(reservedWriteBatchSize);
		this.db = db;
		this.cap = cap;
		this.writeOptions = writeOptions;
		this.setMaxBytes(maxWriteBatchSize);
	}

	private synchronized void flushIfNeeded(boolean force) throws RocksDBException {
		if (this.count() >= (force ? 1 : cap)) {
			db.write(writeOptions, this.getWriteBatch());
			this.clear();
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
			Buf keyToReceive,
			Buf valueToReceive) throws RocksDBException {
		super.put(columnFamilyHandle, asArray(keyToReceive), asArray(valueToReceive));
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

	public synchronized void delete(ColumnFamilyHandle columnFamilyHandle, Buf keyToDelete) throws RocksDBException {
		super.delete(columnFamilyHandle, asArray(keyToDelete));
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
		flushIfNeeded(true);
		super.close();
	}

	public void flush() throws RocksDBException {
		flushIfNeeded(true);
	}
}
