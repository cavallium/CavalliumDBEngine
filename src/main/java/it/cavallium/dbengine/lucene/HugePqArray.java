package it.cavallium.dbengine.lucene;

import io.netty5.buffer.api.Buffer;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.database.disk.HugePqEnv;
import it.cavallium.dbengine.database.disk.StandardRocksDBColumn;
import it.cavallium.dbengine.database.disk.UnreleasableReadOptions;
import it.cavallium.dbengine.database.disk.UnreleasableWriteOptions;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

public class HugePqArray<V> implements IArray<V>, SafeCloseable {

	static {
		RocksDB.loadLibrary();
	}

	private final AtomicBoolean closed = new AtomicBoolean();
	private final HugePqCodec<V> valueCodec;
	private final LLTempHugePqEnv tempEnv;
	private final HugePqEnv env;
	private final int hugePqId;
	private final StandardRocksDBColumn rocksDB;
	private static final UnreleasableWriteOptions writeOptions = new UnreleasableWriteOptions(new WriteOptions()
			.setDisableWAL(true)
			.setSync(false));
	private static final UnreleasableReadOptions readOptions = new UnreleasableReadOptions(new ReadOptions()
			.setVerifyChecksums(false));
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
		try (var valueBuf = valueCodec.serialize(this::allocate, value); keyBuf) {
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
			keyBuf.writeLong(index);
			try (var value = rocksDB.get(readOptions, keyBuf)) {
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
	public void close() {
		if (closed.compareAndSet(false, true)) {
			ensureThread();
			this.tempEnv.freeDb(hugePqId);
		}
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
