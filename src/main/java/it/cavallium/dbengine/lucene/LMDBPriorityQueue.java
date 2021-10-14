package it.cavallium.dbengine.lucene;

import static org.lmdbjava.DbiFlags.*;

import io.net5.buffer.ByteBuf;
import io.net5.buffer.PooledByteBufAllocator;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.CursorIterable.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.GetOp;
import org.lmdbjava.Txn;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

public class LMDBPriorityQueue<T> implements PriorityQueue<T> {

	private static final boolean FORCE_SYNC = false;
	private static final boolean FORCE_THREAD_LOCAL = true;

	private static final AtomicLong NEXT_LMDB_QUEUE_ID = new AtomicLong(0);
	private static final AtomicLong NEXT_ITEM_UID = new AtomicLong(0);

	private final AtomicBoolean closed = new AtomicBoolean();
	private final Runnable onClose;
	private final LMDBCodec<T> codec;
	private final Env<ByteBuf> env;
	private final Dbi<ByteBuf> lmdb;
	private final Scheduler scheduler = Schedulers.newBoundedElastic(1,
			Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE, LMDBThread::new, Integer.MAX_VALUE);

	private boolean writing;
	private boolean iterating;
	private Txn<ByteBuf> readTxn;
	private Txn<ByteBuf> rwTxn;
	private Cursor<ByteBuf> cur;

	private boolean topValid = true;
	private T top = null;
	private long size = 0;

	public LMDBPriorityQueue(LLTempLMDBEnv env, LMDBCodec<T> codec) {
		this.onClose = env::decrementRef;
		var name = "$queue_" + NEXT_LMDB_QUEUE_ID.getAndIncrement();
		this.codec = codec;
		this.env = env.getEnvAndIncrementRef();
		this.lmdb = this.env.openDbi(name, codec::compareDirect, MDB_CREATE, MDB_DUPSORT, MDB_DUPFIXED);
		
		this.writing = true;
		this.iterating = false;
		if (FORCE_THREAD_LOCAL) {
			this.rwTxn = null;
		} else {
			this.rwTxn = this.env.txnWrite();
		}
		this.readTxn = null;
		this.cur = null;
	}

	private ByteBuf allocate(int size) {
		return PooledByteBufAllocator.DEFAULT.directBuffer(size, size);
	}

	private void switchToMode(boolean write, boolean wantCursor) {
		if (iterating) {
			throw new IllegalStateException("Tried to " + (write ? "write" : "read") + " while still iterating");
		}
		boolean changedMode = false;
		if (write) {
			if (!writing) {
				changedMode = true;
				writing = true;
				if (cur != null) {
					cur.close();
					cur = null;
				}
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
				changedMode = true;
				writing = false;
				if (cur != null) {
					cur.close();
					cur = null;
				}
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

		if (cur == null) {
			if (wantCursor) {
				cur = lmdb.openCursor(Objects.requireNonNull(writing ? rwTxn : readTxn));
			}
		} else {
			if (changedMode) {
				cur.close();
				cur = null;
			}
		}
	}

	private void endMode() {
		if (FORCE_THREAD_LOCAL) {
			if (cur != null) {
				cur.close();
				cur = null;
			}
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
		assert cur == null;
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
	public void add(T element) {
		ensureThread();
		switchToMode(true, false);
		var buf = codec.serialize(this::allocate, element);
		var uid = allocate(Long.BYTES);
		uid.writeLong(NEXT_ITEM_UID.getAndIncrement());
		try {
			if (lmdb.put(rwTxn, buf, uid)) {
				if (++size == 1) {
					topValid = true;
					top = element;
				} else {
					topValid = false;
				}
			}
		} finally {
			endMode();
		}

		assert topSingleValid(element);
	}

	private boolean topSingleValid(T element) {
		if (size == 1) {
			var top = databaseTop();
			return codec.compare(top, element) == 0;
		} else {
			return true;
		}
	}

	@Override
	public T top() {
		ensureThread();
		if (topValid) {
			return top;
		} else {
			var top = databaseTop();
			this.top = top;
			topValid = true;
			return top;
		}
	}

	private T databaseTop() {
		ensureThread();
		switchToMode(false, true);
		try {
			if (cur.first()) {
				return codec.deserialize(cur.key());
			} else {
				return null;
			}
		} finally {
			endMode();
		}
	}

	@Override
	public T pop() {
		ensureThread();
		switchToMode(true, true);
		try {
			if (cur.first()) {
				var data = codec.deserialize(cur.key());
				if (--size == 0) {
					topValid = true;
					top = null;
				} else {
					topValid = false;
					top = null;
				}
				cur.delete();
				return data;
			} else {
				topValid = true;
				top = null;
				return null;
			}
		} finally {
			endMode();
		}
	}

	@Override
	public void replaceTop(T newTop) {
		ensureThread();
		this.pop();
		this.add(newTop);
	}

	@Override
	public long size() {
		ensureThread();
		return size;
	}

	@Override
	public void clear() {
		ensureThread();
		switchToMode(true, false);
		try {
			lmdb.drop(rwTxn);
			topValid = true;
			top = null;
			size = 0;
		} finally {
			endMode();
		}
	}

	@Override
	public boolean remove(@NotNull T element) {
		ensureThread();
		Objects.requireNonNull(element);
		switchToMode(true, true);
		var buf = codec.serialize(this::allocate, element);
		try {
			var deletable = cur.get(buf, GetOp.MDB_SET);
			if (deletable) {
				cur.delete();
				if (topValid && codec.compare(top, element) == 0) {
					if (--size == 0) {
						top = null;
					}
				} else {
					if (--size == 0) {
						topValid = true;
						top = null;
					} else {
						topValid = false;
					}
				}
			}
			return deletable;
		} finally {
			endMode();
		}
	}

	@Override
	public Flux<T> iterate() {
		return Flux
				.<T, Tuple2<CursorIterable<ByteBuf>, Iterator<KeyVal<ByteBuf>>>>generate(() -> {
					ensureItThread();
					switchToMode(false, false);
					iterating = true;
					if (cur != null) {
						cur.close();
						cur = null;
					}
					CursorIterable<ByteBuf> cit = lmdb.iterate(readTxn);
					var it = cit.iterator();
					return Tuples.of(cit, it);
				}, (t, sink) -> {
					try {
						ensureItThread();
						var it = t.getT2();
						if (it.hasNext()) {
							sink.next(codec.deserialize(it.next().key()));
						} else {
							sink.complete();
						}
						return t;
					} catch (Throwable ex) {
						sink.error(ex);
						return t;
					}
				}, t -> {
					ensureItThread();
					var cit = t.getT1();
					cit.close();
					iterating = false;
					endMode();
				});
	}

	@Override
	public Flux<T> iterate(long skips) {
		return Flux
				.<T, Tuple3<CursorIterable<ByteBuf>, Iterator<KeyVal<ByteBuf>>, Long>>generate(() -> {
					ensureItThread();
					switchToMode(false, false);
					iterating = true;
					if (cur != null) {
						cur.close();
						cur = null;
					}
					CursorIterable<ByteBuf> cit = lmdb.iterate(readTxn);
					var it = cit.iterator();
					return Tuples.of(cit, it, skips);
				}, (t, sink) -> {
					ensureItThread();
					var it = t.getT2();
					var remainingSkips = t.getT3();
					while (remainingSkips-- > 0 && it.hasNext()) {
						it.next();
					}
					if (it.hasNext()) {
						sink.next(codec.deserialize(it.next().key()));
					} else {
						sink.complete();
					}
					return t.getT3() == 0L ? t : t.mapT3(s -> 0L);
				}, t -> {
					ensureItThread();
					var cit = t.getT1();
					cit.close();
					iterating = false;
					endMode();
				})
				.subscribeOn(scheduler, false);
	}

	@Override
	public void close() throws IOException {
		if (closed.compareAndSet(false, true)) {
			try {
				ensureThread();
				if (cur != null) {
					cur.close();
				}
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
		scheduler.dispose();
	}

	public Scheduler getScheduler() {
		return scheduler;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LMDBPriorityQueue.class.getSimpleName() + "[", "]")
				.add("size=" + size)
				.toString();
	}
}
