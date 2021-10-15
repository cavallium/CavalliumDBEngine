package it.cavallium.dbengine.lucene;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

import io.net5.buffer.ByteBuf;
import io.net5.buffer.PooledByteBufAllocator;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.CursorIterable.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.GetOp;
import org.lmdbjava.Txn;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

public class LMDBArray<V> implements IArray<V>, Closeable {

	private static final boolean FORCE_SYNC = false;
	private static final boolean FORCE_THREAD_LOCAL = true;

	private static final AtomicLong NEXT_LMDB_ARRAY_ID = new AtomicLong(0);

	private final AtomicBoolean closed = new AtomicBoolean();
	private final Runnable onClose;
	private final LMDBCodec<V> valueCodec;
	private final Env<ByteBuf> env;
	private final Dbi<ByteBuf> lmdb;
	private final V defaultValue;

	private boolean writing;
	private Txn<ByteBuf> readTxn;
	private Txn<ByteBuf> rwTxn;

	private long allocatedSize = 0;
	private final long virtualSize;

	public LMDBArray(LLTempLMDBEnv env, LMDBCodec<V> codec, long size, @Nullable V defaultValue) {
		this.onClose = env::decrementRef;
		var name = "$array_" + NEXT_LMDB_ARRAY_ID.getAndIncrement();
		this.valueCodec = codec;
		this.env = env.getEnvAndIncrementRef();
		this.lmdb = this.env.openDbi(name, MDB_CREATE);
		this.defaultValue = defaultValue;
		
		this.writing = true;
		if (FORCE_THREAD_LOCAL) {
			this.rwTxn = null;
		} else {
			this.rwTxn = this.env.txnWrite();
		}
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
				if (FORCE_SYNC) {
					env.sync(true);
				}
				assert rwTxn == null;
				assert readTxn == null;
				readTxn = env.txnRead();
			}
		}
	}

	private void endMode() {
		if (FORCE_THREAD_LOCAL) {
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
		}
		assert rwTxn == null;
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
		switchToMode(true);
		var keyBuf = allocate(Long.BYTES);
		var valueBuf = valueCodec.serialize(this::allocate, value);
		keyBuf.writeLong(index);
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
		switchToMode(false);
		var keyBuf = allocate(Long.BYTES);
		keyBuf.writeLong(index);
		try {
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
			try {
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
			} finally {
				onClose.run();
			}
		}
	}


	@Override
	public String toString() {
		return "lmdb_array[" + virtualSize + " (allocated=" + allocatedSize + ")]";
	}
}
