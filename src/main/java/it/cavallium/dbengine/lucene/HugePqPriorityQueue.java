package it.cavallium.dbengine.lucene;

import io.netty5.buffer.api.Buffer;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.database.disk.HugePqEnv;
import it.cavallium.dbengine.database.disk.RocksIterWithReadOpts;
import it.cavallium.dbengine.database.disk.StandardRocksDBColumn;
import it.cavallium.dbengine.database.disk.UpdateAtomicResultMode;
import it.cavallium.dbengine.database.disk.UpdateAtomicResultPrevious;
import it.cavallium.dbengine.utils.SimpleResource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import reactor.core.publisher.Flux;

public class HugePqPriorityQueue<T> extends SimpleResource
		implements PriorityQueue<T>, Reversable<ReversableResourceIterable<T>>, ReversableResourceIterable<T> {

	static {
		RocksDB.loadLibrary();
	}

	private final LLTempHugePqEnv tempEnv;
	private final HugePqEnv env;
	private final int hugePqId;
	private final StandardRocksDBColumn rocksDB;
	private final HugePqCodec<T> codec;

	private long size = 0;

	private T cachedTop;
	private boolean cachedTopSet = false;

	public HugePqPriorityQueue(LLTempHugePqEnv env, HugePqCodec<T> codec) {
		this.tempEnv = env;
		this.env = env.getEnv();
		this.hugePqId = env.allocateDb(codec.getComparator());
		this.rocksDB = this.env.openDb(hugePqId);
		this.codec = codec;
	}

	private static ReadOptions newReadOptions() {
		return new ReadOptions().setVerifyChecksums(false);
	}

	private static WriteOptions newWriteOptions() {
		return new WriteOptions().setDisableWAL(true).setSync(false);
	}

	private Buffer allocate(int size) {
		return rocksDB.getAllocator().allocate(size);
	}

	private static void ensureThread() {
		LLUtils.ensureBlocking();
	}

	private static void ensureItThread() {
		ensureThread();
	}

	@Override
	public void add(T element) {
		ensureThread();

		cachedTopSet = false;
		var keyBuf = serializeKey(element);
		try (keyBuf) {
			try (var readOptions = newReadOptions(); var writeOptions = newWriteOptions()) {
				rocksDB
						.updateAtomic(readOptions, writeOptions, keyBuf, this::incrementOrAdd, UpdateAtomicResultMode.NOTHING)
						.close();
			}
			++size;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private Buffer serializeKey(T element) {
		return codec.serialize(this::allocate, element);
	}

	private T deserializeKey(Buffer keyBuf) {
		return codec.deserialize(keyBuf.writerOffset(keyBuf.writerOffset()));
	}

	private Buffer serializeValue(int count) {
		var keyBuf = allocate(Integer.BYTES);
		keyBuf.writeInt(count);
		return keyBuf;
	}

	private int deserializeValue(Buffer keyBuf) {
		return keyBuf.readInt();
	}

	@Override
	public T top() {
		if (cachedTopSet) {
			return cachedTop;
		}
		ensureThread();
		return databaseTop();
	}

	private T databaseTop() {
		try (var readOptions = newReadOptions();
				var it = rocksDB.newRocksIterator(true, readOptions, LLRange.all(), false)) {
			it.seekToFirst();
			if (it.isValid()) {
				var key = it.key();
				try (var keyBuf = rocksDB.getAllocator().copyOf(key)) {
					var top = deserializeKey(keyBuf);
					cachedTop = top;
					cachedTopSet = true;
					return top;
				}
			} else {
				cachedTop = null;
				cachedTopSet = true;
				return null;
			}
		} catch (RocksDBException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public T pop() {
		ensureThread();
		cachedTopSet = false;
		try (var readOptions = newReadOptions();
				var writeOptions = newWriteOptions();
				var it  = rocksDB.newRocksIterator(true, readOptions, LLRange.all(), false)) {
			it.seekToFirst();
			if (it.isValid()) {
				var key = it.key();
				try (var keyBuf = rocksDB.getAllocator().copyOf(key)) {
					rocksDB
							.updateAtomic(readOptions, writeOptions, keyBuf, this::reduceOrRemove, UpdateAtomicResultMode.NOTHING)
							.close();
					--size;
					return deserializeKey(keyBuf);
				}
			} else {
				return null;
			}
		} catch (RocksDBException | IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private Buffer incrementOrAdd(@Nullable Buffer prev) {
		if (prev == null) {
			return serializeValue(1);
		} else {
			var prevCount = deserializeValue(prev);
			assert prevCount > 0;
			return serializeValue(prevCount + 1);
		}
	}

	@Nullable
	private Buffer reduceOrRemove(@Nullable Buffer prev) {
		if (prev == null) {
			return null;
		}
		var prevCount = deserializeValue(prev);
		assert prevCount > 0;
		if (prevCount == 1) {
			return null;
		} else {
			return serializeValue(prevCount - 1);
		}
	}

	@Override
	public void replaceTop(T oldTop, T newTop) {
		ensureThread();
		cachedTopSet = false;
		if (oldTop == null) {
			add(newTop);
			cachedTop = newTop;
			cachedTopSet = true;
		} else {
			try (var readOptions = newReadOptions();
					var writeOptions = newWriteOptions();
					var oldKeyBuf = serializeKey(oldTop);
					var newKeyBuf = serializeKey(newTop);
					var ignored = rocksDB.updateAtomic(readOptions,
							writeOptions,
							oldKeyBuf,
							this::reduceOrRemove,
							UpdateAtomicResultMode.NOTHING
					);
					var ignored2 = rocksDB.updateAtomic(readOptions,
							writeOptions,
							newKeyBuf,
							this::incrementOrAdd,
							UpdateAtomicResultMode.NOTHING
					)) {
				cachedTop = newTop;
				cachedTopSet = true;
			} catch (IOException ex) {
				throw new IllegalStateException(ex);
			}
		}
	}

	@Override
	public long size() {
		ensureThread();
		return size;
	}

	@Override
	public void clear() {
		ensureThread();
		cachedTopSet = false;
		try (var wb = new WriteBatch(); var wo = newWriteOptions()) {
			wb.deleteRange(rocksDB.getColumnFamilyHandle(), new byte[0], getBiggestKey());
			size = 0;
			rocksDB.write(wo, wb);
		} catch (RocksDBException e) {
			throw new IllegalStateException(e);
		}
	}

	private byte[] getBiggestKey() {
		var biggestKey = new byte[4096];
		Arrays.fill(biggestKey, (byte) 0xFF);
		return biggestKey;
	}

	@Override
	public boolean remove(@NotNull T element) {
		ensureThread();
		Objects.requireNonNull(element);
		cachedTopSet = false;
		try (var readOptions = newReadOptions();
				var writeOptions = newWriteOptions();
				var keyBuf = serializeKey(element)) {
			try (var prev = (UpdateAtomicResultPrevious) rocksDB.updateAtomic(readOptions, writeOptions,
					keyBuf,
					this::reduceOrRemove,
					UpdateAtomicResultMode.PREVIOUS
			)) {
				if (prev.previous() != null) {
					--size;
					return true;
				} else {
					return false;
				}
			}
		} catch (IOException ex) {
			throw new IllegalStateException(ex);
		}
	}

	public Flux<T> reverseIterate() {
		return iterate(0, true);
	}

	@Override
	public Flux<T> iterate() {
		return iterate(0, false);
	}

	private Flux<T> iterate(long skips, boolean reverse) {
		return Flux.<List<T>, RocksIterWithReadOpts>generate(() -> {
			var readOptions = newReadOptions();
			var rocksIterator = rocksDB.newRocksIterator(true, readOptions, LLRange.all(), reverse);
			if (reverse) {
				rocksIterator.seekToLast();
			} else {
				rocksIterator.seekToFirst();
			}
			long skipsDone = 0;
			while (rocksIterator.isValid() && skipsDone < skips) {
				if (reverse) {
					rocksIterator.prev();
				} else {
					rocksIterator.next();
				}
				skipsDone++;
			}
			return new RocksIterWithReadOpts(readOptions, rocksIterator);
		}, (t, sink) -> {
			var rocksIterator = t.iter();
			if (rocksIterator.isValid()) {
				try (var keyBuf = rocksDB.getAllocator().copyOf(rocksIterator.key());
						var valBuf = rocksDB.getAllocator().copyOf(rocksIterator.value())) {
					var count = deserializeValue(valBuf);
					if (count == 0) {
						sink.next(List.of());
					} else {
						var result = new ArrayList<T>(count);
						T origKey = deserializeKey(keyBuf);
						for (int i = 0; i < count; i++) {
							if (i == 0) {
								result.add(origKey);
							} else {
								result.add(codec.clone(origKey));
							}
						}
						sink.next(result);
					}
				}
				try {
					if (reverse) {
						rocksIterator.prev();
					} else {
						rocksIterator.next();
					}
				} catch (RocksDBException e) {
					sink.error(e);
				}
			} else {
				sink.complete();
			}

			return t;
		}, rocksIterWithReadOpts -> {
			if (rocksIterWithReadOpts != null) {
				rocksIterWithReadOpts.close();
			}
		}).concatMapIterable(item -> item);
	}

	@Override
	public Flux<T> iterate(long skips) {
		return iterate(skips, false);
	}

	public Flux<T> reverseIterate(long skips) {
		return iterate(skips, true);
	}

	@Override
	protected void onClose() {
		this.tempEnv.freeDb(hugePqId);
		if (this.codec instanceof SafeCloseable closeable) {
			closeable.close();
		}
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", HugePqPriorityQueue.class.getSimpleName() + "[", "]")
				.add("size=" + size)
				.toString();
	}

	@Override
	public ReversableResourceIterable<T> reverse() {
		return new ReversedResourceIterable();
	}

	private class ReversedResourceIterable extends SimpleResource implements ReversableResourceIterable<T> {

		@Override
		public void onClose() {
			HugePqPriorityQueue.this.close();
		}

		@Override
		public Flux<T> iterate() {
			return reverseIterate();
		}

		@Override
		public Flux<T> iterate(long skips) {
			return reverseIterate(skips);
		}

		@Override
		public ReversableResourceIterable<T> reverse() {
			return HugePqPriorityQueue.this;
		}
	}
}
