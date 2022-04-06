package it.cavallium.dbengine.lucene;

import io.netty5.buffer.api.Buffer;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.database.disk.HugePqEnv;
import it.cavallium.dbengine.database.disk.StandardRocksDBColumn;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

public class HugePqArray<V> implements IArray<V>, SafeCloseable {


	private final AtomicBoolean closed = new AtomicBoolean();
	private final HugePqCodec<V> valueCodec;
	private final LLTempHugePqEnv tempEnv;
	private final HugePqEnv env;
	private final int hugePqId;
	private final StandardRocksDBColumn rocksDB;
	private WriteOptions writeOptions;
	private ReadOptions readOptions;
	private final V defaultValue;

	private final long virtualSize;

	public HugePqArray(LLTempHugePqEnv env, HugePqCodec<V> codec, long size, @Nullable V defaultValue) {
		this.valueCodec = codec;
		this.tempEnv = env;
		this.env = env.getEnv();
		this.hugePqId = env.allocateDb(null);
		this.rocksDB = this.env.openDb(hugePqId);
		this.writeOptions = new WriteOptions().setDisableWAL(true).setSync(false);
		this.readOptions = new ReadOptions().setVerifyChecksums(false);
		this.defaultValue = defaultValue;

		this.virtualSize = size;
	}

	public HugePqCodec<V> getValueCodec() {
		return valueCodec;
	}

	private Buffer allocate(int size) {
		return rocksDB.getAllocator().allocate(size);
	}

	private static void ensureThread() {
		LLUtils.ensureBlocking();
	}

	@Override
	public void set(long index, @Nullable V value) {
		ensureBounds(index);
		ensureThread();
		var keyBuf = allocate(Long.BYTES);
		keyBuf.writeLong(index);
		try (var valueBuf = valueCodec.serialize(this::allocate, value); keyBuf) {
			rocksDB.put(writeOptions, keyBuf, valueBuf);
		} catch (RocksDBException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void reset(long index) {
		ensureBounds(index);
		ensureThread();
		var keyBuf = allocate(Long.BYTES);
		try (keyBuf) {
			keyBuf.writeLong(index);
			rocksDB.delete(writeOptions, keyBuf);
		} catch (RocksDBException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public @Nullable V get(long index) {
		ensureBounds(index);
		ensureThread();

		var keyBuf = allocate(Long.BYTES);
		try (keyBuf) {
			try (var value = rocksDB.get(readOptions, keyBuf)) {
				return valueCodec.deserialize(value);
			}
		} catch (RocksDBException e) {
			throw new IllegalStateException(e);
		}
	}

	private void ensureBounds(long index) {
		if (index < 0 || index >= virtualSize) throw new IndexOutOfBoundsException();
	}

	@Override
	public long size() {
		ensureThread();
		return virtualSize;
	}

	@Override
	public void close() {
		readOptions.close();
		writeOptions.close();
		if (closed.compareAndSet(false, true)) {
			ensureThread();
			this.tempEnv.freeDb(hugePqId);
		}
	}


	@Override
	public String toString() {
		return "huge_pq_array[" + virtualSize + "]";
	}
}
