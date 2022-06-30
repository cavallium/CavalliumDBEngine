package it.cavallium.dbengine.lucene;

import io.netty5.buffer.api.Buffer;
import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.database.disk.HugePqEnv;
import it.cavallium.dbengine.database.disk.StandardRocksDBColumn;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

public class HugePqArray<V> extends SimpleResource implements IArray<V>, DiscardingCloseable {

	static {
		RocksDB.loadLibrary();
	}

	private final HugePqCodec<V> valueCodec;
	private final LLTempHugePqEnv tempEnv;
	private final HugePqEnv env;
	private final int hugePqId;
	private final StandardRocksDBColumn rocksDB;
	private final V defaultValue;

	private final long virtualSize;

	public HugePqArray(LLTempHugePqEnv env, HugePqCodec<V> codec, long size, @Nullable V defaultValue) {
		this.valueCodec = codec;
		this.tempEnv = env;
		this.env = env.getEnv();
		this.hugePqId = env.allocateDb(null);
		this.rocksDB = this.env.openDb(hugePqId);
		this.defaultValue = defaultValue;

		this.virtualSize = size;
	}

	private static ReadOptions newReadOptions() {
		return new ReadOptions().setVerifyChecksums(false);
	}

	private static WriteOptions newWriteOptions() {
		return new WriteOptions().setDisableWAL(true).setSync(false);
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
		try (var writeOptions = newWriteOptions();
				var valueBuf = valueCodec.serialize(this::allocate, value); keyBuf) {
			keyBuf.writeLong(index);
			rocksDB.put(writeOptions, keyBuf, valueBuf);
		} catch (RocksDBException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void reset(long index) {
		ensureBounds(index);
		ensureThread();
		try (var writeOptions = newWriteOptions();
				var keyBuf = allocate(Long.BYTES)) {
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
			keyBuf.writeLong(index);
			try (var readOptions = newReadOptions();
					var value = rocksDB.get(readOptions, keyBuf)) {
				if (value == null) {
					return null;
				}
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
	public void onClose() {
		ensureThread();
		this.tempEnv.freeDb(hugePqId);
	}

	@Override
	public String toString() {
		return "huge_pq_array[" + virtualSize + "]";
	}

	public Object[] toArray() {
		var result = new Object[Math.toIntExact(virtualSize)];
		for (int i = 0; i < virtualSize; i++) {
			result[i] = get(i);
		}
		return result;
	}
}
