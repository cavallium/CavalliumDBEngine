package it.cavallium.dbengine.lucene;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

import io.net5.buffer.ByteBuf;
import io.net5.buffer.PooledByteBufAllocator;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

public class LMDBArray<V> implements IArray<V>, Closeable {

	private static final AtomicLong NEXT_LMDB_ARRAY_ID = new AtomicLong(0);

	private final AtomicBoolean closed = new AtomicBoolean();
	private final LMDBCodec<V> valueCodec;
	private final Env<ByteBuf> env;
	private final Dbi<ByteBuf> lmdb;
	private final V defaultValue;

	private boolean writing;
	private Txn<ByteBuf> readTxn;
	private Txn<ByteBuf> rwTxn;

	// Cache
	private static final int WRITE_QUEUE_MAX_BOUND = 10_000;
	private final Deque<ByteBuf> toWriteKeys = new ArrayDeque<>();
	private final Deque<ByteBuf> toWriteValues = new ArrayDeque<>();

	private long allocatedSize = 0;
	private final long virtualSize;

	public LMDBArray(LLTempLMDBEnv env, LMDBCodec<V> codec, long size, @Nullable V defaultValue) {
		var name = "$array_" + NEXT_LMDB_ARRAY_ID.getAndIncrement();
		this.valueCodec = codec;
		this.env = env.getEnv();
		this.lmdb = this.env.openDbi(name, MDB_CREATE);
		this.defaultValue = defaultValue;
		
		this.writing = true;
		this.rwTxn = null;
		this.readTxn = null;
		this.virtualSize = size;
	}

	public LMDBCodec<V> getValueCodec() {
		return valueCodec;
	}

	private ByteBuf allocate(int size) {
		return PooledByteBufAllocator.DEFAULT.directBuffer(size, size);
	}

	private void switchToMode(boolean write) {
		if (toWriteKeys.size() > 0) {
			switchToModeUncached(true);
			try {
				var ki = toWriteKeys.iterator();
				var vi = toWriteValues.iterator();
				while (ki.hasNext()) {
					var k = ki.next();
					var v = vi.next();
					if (lmdb.put(rwTxn, k, v)) {
						allocatedSize++;
					}
				}
			} finally {
				endMode();
				for (ByteBuf toWriteKey : toWriteKeys) {
					toWriteKey.release();
				}
				for (ByteBuf toWriteValue : toWriteValues) {
					toWriteValue.release();
				}
				toWriteKeys.clear();
				toWriteValues.clear();
			}
		}

		switchToModeUncached(write);
	}

	private void switchToModeUncached(boolean write) {
		if (write) {
			if (!writing) {
				writing = true;
				readTxn.close();
				readTxn = null;
				assert rwTxn == null;
				rwTxn = env.txnWrite();
			} else if (rwTxn == null) {
				assert readTxn == null;
				rwTxn = env.txnWrite();
			}
		} else {
			if (writing) {
				writing = false;
				if (rwTxn != null) {
					rwTxn.commit();
					rwTxn.close();
					rwTxn = null;
				}
				assert readTxn == null;
				readTxn = env.txnRead();
			}
		}
	}

	private void endMode() {
		writing = true;
		if (readTxn != null) {
			readTxn.commit();
			readTxn.close();
			readTxn = null;
		}
		if (rwTxn != null) {
			rwTxn.commit();
			rwTxn.close();
			rwTxn = null;
		}
		assert readTxn == null;
	}

	private static void ensureThread() {
		LLUtils.ensureBlocking();
	}

	private static void ensureItThread() {
		ensureThread();
		//if (!(Thread.currentThread() instanceof LMDBThread)) {
  	//		throw new IllegalStateException("Must run in LMDB scheduler");
		//}
	}

	@Override
	public void set(long index, @Nullable V value) {
		ensureBounds(index);
		ensureThread();
		var keyBuf = allocate(Long.BYTES);
		var valueBuf = valueCodec.serialize(this::allocate, value);
		keyBuf.writeLong(index);
		if (toWriteKeys.size() < WRITE_QUEUE_MAX_BOUND) {
			toWriteKeys.add(keyBuf);
			toWriteValues.add(valueBuf);
		} else {
			switchToMode(true);
			try {
				if (lmdb.put(rwTxn, keyBuf, valueBuf)) {
					allocatedSize++;
				}
			} finally {
				endMode();

				keyBuf.release();
				valueBuf.release();
			}
		}
	}

	@Override
	public void reset(long index) {
		ensureBounds(index);
		ensureThread();
		switchToMode(true);
		var keyBuf = allocate(Long.BYTES);
		keyBuf.writeLong(index);
		try {
			if (lmdb.delete(rwTxn, keyBuf)) {
				allocatedSize--;
			}
		} finally {
			endMode();
			keyBuf.release();
		}
	}

	@Override
	public @Nullable V get(long index) {
		ensureBounds(index);
		ensureThread();

		if (!toWriteKeys.isEmpty()) {
			var ki = toWriteKeys.iterator();
			var vi = toWriteValues.iterator();
			while (ki.hasNext()) {
				var k = ki.next();
				var v = vi.next();
				var readIndex = k.getLong(0);
				if (readIndex == index) {
					var ri = v.readerIndex();
					var wi = v.writerIndex();
					var c = v.capacity();
					try {
						return valueCodec.deserialize(v);
					} finally {
						v.readerIndex(ri);
						v.writerIndex(wi);
						v.capacity(c);
					}
				}
			}
		}

		var keyBuf = allocate(Long.BYTES);
		keyBuf.writeLong(index);
		try {
			switchToModeUncached(false);
			var value = lmdb.get(readTxn, keyBuf);
			if (value != null) {
				return valueCodec.deserialize(value);
			} else {
				return defaultValue;
			}
		} finally {
			endMode();
			keyBuf.release();
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

	public long allocatedSize() {
		return allocatedSize;
	}

	@Override
	public void close() throws IOException {
		if (closed.compareAndSet(false, true)) {
			ensureThread();
			if (rwTxn != null) {
				rwTxn.close();
			}
			if (readTxn != null) {
				readTxn.close();
			}
			try (var txn = env.txnWrite()) {
				lmdb.drop(txn, true);
				txn.commit();
			}
			lmdb.close();
		}
	}


	@Override
	public String toString() {
		return "lmdb_array[" + virtualSize + " (allocated=" + allocatedSize + ")]";
	}
}
