package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.Holder;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;
import org.warp.commonutils.functional.CancellableBiConsumer;
import org.warp.commonutils.functional.CancellableBiFunction;
import org.warp.commonutils.functional.ConsumerResult;

@NotAtomic
public class LLLocalDictionary implements LLDictionary {

	private static final boolean USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS = true;
	static final int RESERVED_WRITE_BATCH_SIZE = 2 * 1024 * 1024; // 2MiB
	static final long MAX_WRITE_BATCH_SIZE = 1024L * 1024L * 1024L; // 1GiB
	static final int CAPPED_WRITE_BATCH_CAP = 50000; // 50K operations
	static final WriteOptions BATCH_WRITE_OPTIONS = new WriteOptions().setLowPri(true);

	private static final byte[] NO_DATA = new byte[0];
	private static final ReadOptions EMPTY_READ_OPTIONS = new ReadOptions();
	private static final List<byte[]> EMPTY_UNMODIFIABLE_LIST = List.of();
	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final String databaseName;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;

	public LLLocalDictionary(@NotNull RocksDB db,
			@NotNull ColumnFamilyHandle columnFamilyHandle,
			String databaseName,
			Function<LLSnapshot, Snapshot> snapshotResolver) {
		Objects.requireNonNull(db);
		this.db = db;
		Objects.requireNonNull(columnFamilyHandle);
		this.cfh = columnFamilyHandle;
		this.databaseName = databaseName;
		this.snapshotResolver = snapshotResolver;
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}

	private ReadOptions resolveSnapshot(LLSnapshot snapshot) {
		if (snapshot != null) {
			return getReadOptions(snapshotResolver.apply(snapshot));
		} else {
			return EMPTY_READ_OPTIONS;
		}
	}

	private ReadOptions getReadOptions(Snapshot snapshot) {
		if (snapshot != null) {
			return new ReadOptions().setSnapshot(snapshot);
		} else {
			return EMPTY_READ_OPTIONS;
		}
	}

	@Override
	public Optional<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key) throws IOException {
		try {
			Holder<byte[]> data = new Holder<>();
			if (db.keyMayExist(cfh, resolveSnapshot(snapshot), key, data)) {
				if (data.getValue() != null) {
					return Optional.of(data.getValue());
				} else {
					byte[] value = db.get(cfh, resolveSnapshot(snapshot), key);
					return Optional.ofNullable(value);
				}
			} else {
				return Optional.empty();
			}
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean contains(@Nullable LLSnapshot snapshot, byte[] key) throws IOException {
		return contains_(snapshot, key);
	}

	private boolean contains_(@Nullable LLSnapshot snapshot, byte[] key) throws IOException {
		try {
			int size = RocksDB.NOT_FOUND;
			Holder<byte[]> data = new Holder<>();
			if (db.keyMayExist(cfh, resolveSnapshot(snapshot), key, data)) {
				if (data.getValue() != null) {
					size = data.getValue().length;
				} else {
					size = db.get(cfh, resolveSnapshot(snapshot), key, NO_DATA);
				}
			}
			return size != RocksDB.NOT_FOUND;
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public Optional<byte[]> put(byte[] key, byte[] value, LLDictionaryResultType resultType) throws IOException {
		try {
			byte[] response = null;
			switch (resultType) {
				case VALUE_CHANGED:
					response = LLUtils.booleanToResponse(!contains_(null, key));
					break;
				case PREVIOUS_VALUE:
					var data = new Holder<byte[]>();
					if (db.keyMayExist(cfh, key, data)) {
						if (data.getValue() != null) {
							response = data.getValue();
						} else {
							response = db.get(cfh, key);
						}
					} else {
						response = null;
					}
					break;
			}
			db.put(cfh, key, value);
			return Optional.ofNullable(response);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void putMulti(byte[][] key, byte[][] value, LLDictionaryResultType resultType, Consumer<byte[]> responsesConsumer)
			throws IOException {
		if (key.length == value.length) {
			List<byte[]> responses;
			try (WriteBatch writeBatch = new WriteBatch(RESERVED_WRITE_BATCH_SIZE)) {

				if (resultType == LLDictionaryResultType.VOID) {
					responses = EMPTY_UNMODIFIABLE_LIST;
				} else {
					responses = db.multiGetAsList(newCfhList(cfh, key.length), Arrays.asList(key));
				}

				for (int i = 0; i < key.length; i++) {
					writeBatch.put(cfh, key[i], value[i]);
				}

				db.write(BATCH_WRITE_OPTIONS, writeBatch);
			} catch (RocksDBException e) {
				throw new IOException(e);
			}

			for (byte[] response : responses) {
				responsesConsumer.accept(response);
			}
		} else {
			throw new IOException("Wrong parameters count");
		}
	}

	private static List<ColumnFamilyHandle> newCfhList(ColumnFamilyHandle cfh, int size) {
		var list = new ArrayList<ColumnFamilyHandle>(size);
		for (int i = 0; i < size; i++) {
			list.add(cfh);
		}
		return list;
	}

	@Override
	public Optional<byte[]> remove(byte[] key, LLDictionaryResultType resultType) throws IOException {
		try {
			byte[] response = null;
			switch (resultType) {
				case VALUE_CHANGED:
					response = LLUtils.booleanToResponse(contains_(null, key));
					break;
				case PREVIOUS_VALUE:
					var data = new Holder<byte[]>();
					if (db.keyMayExist(cfh, key, data)) {
						if (data.getValue() != null) {
							response = data.getValue();
						} else {
							response = db.get(cfh, key);
						}
					} else {
						response = null;
					}
					break;
			}
			db.delete(cfh, key);
			return Optional.ofNullable(response);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	//todo: implement parallel forEach
	@Override
	public ConsumerResult forEach(@Nullable LLSnapshot snapshot, int parallelism, CancellableBiConsumer<byte[], byte[]> consumer) {
		try (RocksIterator iter = db.newIterator(cfh, resolveSnapshot(snapshot))) {
			iter.seekToFirst();
			while (iter.isValid()) {
				if (consumer.acceptCancellable(iter.key(), iter.value()).isCancelled()) {
					return ConsumerResult.cancelNext();
				}
				iter.next();
			}
		}
		return ConsumerResult.result();
	}

	//todo: implement parallel replace
	@Override
	public ConsumerResult replaceAll(int parallelism, boolean replaceKeys, CancellableBiFunction<byte[], byte[], Entry<byte[], byte[]>> consumer) throws IOException {
		try {
			try (var snapshot = replaceKeys ? db.getSnapshot() : null) {
				try (RocksIterator iter = db.newIterator(cfh, getReadOptions(snapshot));
						CappedWriteBatch writeBatch = new CappedWriteBatch(db, CAPPED_WRITE_BATCH_CAP, RESERVED_WRITE_BATCH_SIZE, MAX_WRITE_BATCH_SIZE, BATCH_WRITE_OPTIONS)) {

					iter.seekToFirst();

					if (replaceKeys) {
						while (iter.isValid()) {
							writeBatch.delete(cfh, iter.key());

							iter.next();
						}
					}

					iter.seekToFirst();

					while (iter.isValid()) {

						var result = consumer.applyCancellable(iter.key(), iter.value());
						boolean keyDiffers = !Arrays.equals(iter.key(), result.getValue().getKey());
						if (!replaceKeys && keyDiffers) {
							throw new IOException("Tried to replace a key");
						}

						// put if changed or if keys can be swapped/replaced
						if (replaceKeys || !Arrays.equals(iter.value(), result.getValue().getValue())) {
							writeBatch.put(cfh, result.getValue().getKey(), result.getValue().getValue());
						}

						if (result.isCancelled()) {
							// Cancels and discards the write batch
							writeBatch.clear();
							return ConsumerResult.cancelNext();
						}

						iter.next();
					}

					writeBatch.writeToDbAndClose();

					return ConsumerResult.result();
				} finally {
					db.releaseSnapshot(snapshot);
				}
			}
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	// This method is exactly the same of LLLocalDictionary. Remember to keep the code equal
	@Override
	public void clear() throws IOException {
		try (RocksIterator iter = db.newIterator(cfh);
				CappedWriteBatch writeBatch = new CappedWriteBatch(db, CAPPED_WRITE_BATCH_CAP, RESERVED_WRITE_BATCH_SIZE, MAX_WRITE_BATCH_SIZE, BATCH_WRITE_OPTIONS)) {

			iter.seekToFirst();

			while (iter.isValid()) {
				writeBatch.delete(cfh, iter.key());

				iter.next();
			}

			writeBatch.writeToDbAndClose();

			// Compact range
			db.compactRange(cfh);

			db.flush(new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true), cfh);
			db.flushWal(true);

			var finalSize = exactSize(null);
			if (finalSize != 0) {
				throw new IllegalStateException("The dictionary is not empty after calling clear()");
			}
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public long size(@Nullable LLSnapshot snapshot, boolean fast) throws IOException {
		return fast ? fastSize(snapshot) : exactSize(snapshot);
	}

	public long fastSize(@Nullable LLSnapshot snapshot) {
		var rocksdbSnapshot = resolveSnapshot(snapshot);
		if (USE_CURRENT_FASTSIZE_FOR_OLD_SNAPSHOTS || rocksdbSnapshot.snapshot() == null) {
			try {
				return db.getLongProperty(cfh, "rocksdb.estimate-num-keys");
			} catch (RocksDBException e) {
				e.printStackTrace();
				return 0;
			}
		} else {
			long count = 0;
			try (RocksIterator iter = db.newIterator(cfh, rocksdbSnapshot)) {
				iter.seekToFirst();
				// If it's a fast size of a snapshot, count only up to 1000 elements
				while (iter.isValid() && count < 1000) {
					count++;
					iter.next();
				}
				return count;
			}
		}
	}

	public long exactSize(@Nullable LLSnapshot snapshot) {
		long count = 0;
		try (RocksIterator iter = db.newIterator(cfh, resolveSnapshot(snapshot))) {
			iter.seekToFirst();
			while (iter.isValid()) {
				count++;
				iter.next();
			}
			return count;
		}
	}

	@Override
	public boolean isEmpty(@Nullable LLSnapshot snapshot) {
		try (RocksIterator iter = db.newIterator(cfh, resolveSnapshot(snapshot))) {
			iter.seekToFirst();
			if (iter.isValid()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public Optional<Entry<byte[], byte[]>> removeOne() throws IOException {
		try (RocksIterator iter = db.newIterator(cfh)) {
			iter.seekToFirst();
			if (iter.isValid()) {
				byte[] key = iter.key();
				byte[] value = iter.value();
				db.delete(cfh, key);
				return Optional.of(Map.entry(key, value));
			}
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
		return Optional.empty();
	}
}
