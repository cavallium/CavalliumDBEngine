package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLDeepDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.ImmutableTriple;
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
import org.rocksdb.WriteBatchInterface;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;
import org.warp.commonutils.error.IndexOutOfBoundsException;
import org.warp.commonutils.functional.CancellableBiConsumer;
import org.warp.commonutils.functional.CancellableBiFunction;
import org.warp.commonutils.functional.CancellableTriConsumer;
import org.warp.commonutils.functional.CancellableTriFunction;
import org.warp.commonutils.functional.ConsumerResult;
import org.warp.commonutils.type.Bytes;
import org.warp.commonutils.type.UnmodifiableIterableMap;
import org.warp.commonutils.type.UnmodifiableMap;

@NotAtomic
public class LLLocalDeepDictionary implements LLDeepDictionary {

	private static final byte[] NO_DATA = new byte[0];
	private static final byte[][] NO_DATA_MAP = new byte[0][0];
	private static final ReadOptions EMPTY_READ_OPTIONS = new ReadOptions();
	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final String databaseName;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;
	private final int key1Size;
	private final int key2Size;
	private final int key1Position;
	private final int key2Position;
	private final int key1EndPosition;
	private final int key2EndPosition;
	private final int combinedKeySize;

	public LLLocalDeepDictionary(@NotNull RocksDB db, @NotNull ColumnFamilyHandle columnFamilyHandle,
			String databaseName,
			Function<LLSnapshot, Snapshot> snapshotResolver, int keySize, int key2Size) {
		Objects.requireNonNull(db);
		this.db = db;
		Objects.requireNonNull(columnFamilyHandle);
		this.cfh = columnFamilyHandle;
		this.databaseName = databaseName;
		this.snapshotResolver = snapshotResolver;
		this.key1Size = keySize;
		this.key2Size = key2Size;
		this.key1Position = 0;
		this.key2Position = key1Size;
		this.key1EndPosition = key1Position + key1Size;
		this.key2EndPosition = key2Position + key2Size;
		this.combinedKeySize = keySize + key2Size;
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	private boolean isSubKey(byte[] key1, byte[] combinedKey) {
		if (key1 == null || combinedKey == null || key1.length != key1Size || combinedKey.length != combinedKeySize) {
			return false;
		}

		return Arrays.equals(key1, 0, key1Size, combinedKey, key1Position, key1EndPosition);
	}

	private byte[] getStartSeekKey(byte[] key1) {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		return Arrays.copyOf(key1, combinedKeySize);
	}

	private byte[] getEndSeekKey(byte[] key1) {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		byte[] endSeekKey = Arrays.copyOf(key1, combinedKeySize);
		Arrays.fill(endSeekKey, key2Position, key2EndPosition, (byte) 0xFF);
		return endSeekKey;
	}

	@NotNull
	private byte[] getKey1(@NotNull byte[] combinedKey) {
		if (combinedKey.length != combinedKeySize) {
			throw new IndexOutOfBoundsException(combinedKey.length, combinedKeySize, combinedKeySize);
		}
		return Arrays.copyOfRange(combinedKey, key1Position, key1EndPosition);
	}

	@NotNull
	private byte[] getKey2(@NotNull byte[] combinedKey) {
		return Arrays.copyOfRange(combinedKey, key2Position, key2EndPosition);
	}

	@NotNull
	private byte[] getCombinedKey(@NotNull byte[] key1, @NotNull byte[] key2) {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		if (key2.length != key2Size) {
			throw new IndexOutOfBoundsException(key2.length, key2Size, key2Size);
		}
		var combinedKey = new byte[combinedKeySize];
		System.arraycopy(key1, 0, combinedKey, key1Position, key1Size);
		System.arraycopy(key2, 0, combinedKey, key2Position, key2Size);
		return combinedKey;
	}

	private ReadOptions resolveSnapshot(LLSnapshot snapshot) {
		if (snapshot != null) {
			return new ReadOptions().setSnapshot(snapshotResolver.apply(snapshot));
		} else {
			return EMPTY_READ_OPTIONS;
		}
	}

	@Override
	public UnmodifiableIterableMap<byte[], byte[]> get(@Nullable LLSnapshot snapshot, byte[] key) throws IOException {
		if (key.length != key1Size) {
			throw new IndexOutOfBoundsException(key.length, key1Size, key1Size);
		}
		ObjectArrayList<byte[]> keys = new ObjectArrayList<>();
		ObjectArrayList<byte[]> values = new ObjectArrayList<>();
		try (var iterator = db.newIterator(cfh, resolveSnapshot(snapshot))) {
			iterator.seek(key);
			while (iterator.isValid()) {

				byte[] combinedKey = iterator.key();

				if (!isSubKey(key, combinedKey)) {
					break;
				}

				byte[] key2 = getKey2(combinedKey);
				byte[] value = iterator.value();
				keys.add(key2);
				values.add(value);

				iterator.next();
			}
		}

		return UnmodifiableIterableMap.of(keys.toArray(byte[][]::new), values.toArray(byte[][]::new));
	}

	@Override
	public Optional<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key1, byte[] key2) throws IOException {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		if (key2.length != key2Size) {
			throw new IndexOutOfBoundsException(key2.length, key2Size, key2Size);
		}
		try {
			Holder<byte[]> data = new Holder<>();
			byte[] combinedKey = getCombinedKey(key1, key2);
			if (db.keyMayExist(cfh, resolveSnapshot(snapshot), combinedKey, data)) {
				if (data.getValue() != null) {
					return Optional.of(data.getValue());
				} else {
					byte[] value = db.get(cfh, resolveSnapshot(snapshot), combinedKey);
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
	public boolean isEmpty(@Nullable LLSnapshot snapshot, byte[] key1) {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		byte[] startSeekKey = getStartSeekKey(key1);
		try (var iterator = db.newIterator(cfh, resolveSnapshot(snapshot))) {
			iterator.seek(startSeekKey);
			if (!iterator.isValid()) {
				return true;
			}
			byte[] startKey = iterator.key();
			return !isSubKey(key1, startKey);
		}
	}

	@Override
	public boolean contains(@Nullable LLSnapshot snapshot, byte[] key1, byte[] key2) throws IOException {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		if (key2.length != key2Size) {
			throw new IndexOutOfBoundsException(key2.length, key2Size, key2Size);
		}
		try {
			var combinedKey = getCombinedKey(key1, key2);
			int size = RocksDB.NOT_FOUND;
			Holder<byte[]> data = new Holder<>();
			if (db.keyMayExist(cfh, resolveSnapshot(snapshot), combinedKey, data)) {
				if (data.getValue() != null) {
					size = data.getValue().length;
				} else {
					size = db.get(cfh, resolveSnapshot(snapshot), combinedKey, NO_DATA);
				}
			}
			return size != RocksDB.NOT_FOUND;
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	//todo: use WriteBatch to enhance performance
	@Override
	public void put(byte[] key1, UnmodifiableIterableMap<byte[], byte[]> value) throws IOException {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		try {
			var bytesValue = Bytes.ofMap(value);
			var alreadyEditedKeys = new ObjectOpenHashSet<Bytes>();

			// Delete old keys and change keys that are already present
			try (var iterator = db.newIterator(cfh)) {
				iterator.seek(getStartSeekKey(key1));
				while (iterator.isValid()) {
					byte[] combinedKey = iterator.key();

					if (!isSubKey(key1, combinedKey)) {
						// The key is outside of key1: exit from the iteration
						break;
					}

					byte[] key2 = getKey2(combinedKey);
					var valueToSetHere = bytesValue.get(key2);
					if (valueToSetHere == null) {
						// key not present in the new data: remove it from the database
						db.delete(cfh, combinedKey);
					} else {
						// key present in the new data: replace it on the database
						alreadyEditedKeys.add(new Bytes(key2));
						db.put(cfh, combinedKey, valueToSetHere.data);
					}

					iterator.next();
				}
			}

			// Add new keys, avoiding to add already changed keys
			var mapIterator = bytesValue.fastIterator();
			while (mapIterator.hasNext()) {
				var mapEntry = mapIterator.next();
				var key2 = mapEntry.getKey();
				if (key2.data.length != key2Size) {
					throw new IndexOutOfBoundsException(key2.data.length, key2Size, key2Size);
				}

				if (!alreadyEditedKeys.contains(key2)) {
					var value2 = mapEntry.getValue();
					db.put(cfh, getCombinedKey(key1, key2.data), value2.data);
				}
			}
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		}
	}

	//todo: use WriteBatch to enhance performance
	@Override
	public void putMulti(byte[][] keys1, UnmodifiableIterableMap<byte[], byte[]>[] values) throws IOException {
		if (keys1.length == values.length) {
			for (int i = 0; i < keys1.length; i++) {
				put(keys1[i], values[i]);
			}
		} else {
			throw new IOException("Wrong parameters count");
		}
	}

	@Override
	public Optional<byte[]> put(byte[] key1, byte[] key2, byte[] value, LLDictionaryResultType resultType)
			throws IOException {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		if (key2.length != key2Size) {
			throw new IndexOutOfBoundsException(key2.length, key2Size, key2Size);
		}
		try {
			byte[] response = null;
			var combinedKey = getCombinedKey(key1, key2);
			switch (resultType) {
				case VALUE_CHANGED:
					response = LLUtils.booleanToResponse(!this.contains(null, key1, key2));
					break;
				case PREVIOUS_VALUE:
					var data = new Holder<byte[]>();
					if (db.keyMayExist(cfh, combinedKey, data)) {
						if (data.getValue() != null) {
							response = data.getValue();
						} else {
							response = db.get(cfh, combinedKey);
						}
					} else {
						response = null;
					}
					break;
			}
			db.put(cfh, combinedKey, value);
			return Optional.ofNullable(response);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	//todo: use WriteBatch to enhance performance
	@Override
	public void putMulti(byte[] key1,
			byte[][] keys2,
			byte[][] values2,
			LLDictionaryResultType resultType,
			Consumer<byte[]> responses) throws IOException {
		if (keys2.length == values2.length) {
			for (int i = 0; i < keys2.length; i++) {
				var result = put(key1, keys2[i], values2[i], resultType);
				if (resultType != LLDictionaryResultType.VOID) {
					responses.accept(result.orElse(NO_DATA));
				}
			}
		} else {
			throw new IOException("Wrong parameters count");
		}
	}

	//todo: use WriteBatch to enhance performance
	@Override
	public void putMulti(byte[][] keys1,
			byte[][] keys2,
			byte[][] values2,
			LLDictionaryResultType resultType,
			Consumer<byte[]> responses) throws IOException {
		if (keys1.length == keys2.length && keys2.length == values2.length) {
			for (int i = 0; i < keys1.length; i++) {
				var result = put(keys1[i], keys2[i], values2[i], resultType);
				if (resultType != LLDictionaryResultType.VOID) {
					responses.accept(result.orElse(NO_DATA));
				}
			}
		} else {
			throw new IOException("Wrong parameters count");
		}
	}

	@Override
	public Optional<byte[]> remove(byte[] key1, byte[] key2, LLDictionaryResultType resultType) throws IOException {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		if (key2.length != key2Size) {
			throw new IndexOutOfBoundsException(key2.length, key2Size, key2Size);
		}
		try {
			byte[] response = null;
			var combinedKey = getCombinedKey(key1, key2);
			switch (resultType) {
				case VALUE_CHANGED:
					response = LLUtils.booleanToResponse(this.contains(null, key1, key2));
					break;
				case PREVIOUS_VALUE:
					var data = new Holder<byte[]>();
					if (db.keyMayExist(cfh, combinedKey, data)) {
						if (data.getValue() != null) {
							response = data.getValue();
						} else {
							response = db.get(cfh, combinedKey);
						}
					} else {
						response = null;
					}
					break;
			}
			db.delete(cfh, combinedKey);
			return Optional.ofNullable(response);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public ConsumerResult forEach(@Nullable LLSnapshot snapshot, int parallelism, CancellableTriConsumer<byte[], byte[], byte[]> consumer) {
		return forEach_(consumer, snapshot == null ? null : snapshotResolver.apply(snapshot), parallelism);
	}

	//todo: implement parallel execution
	private ConsumerResult forEach_(CancellableTriConsumer<byte[], byte[], byte[]> consumer, @Nullable Snapshot snapshot, int parallelism) {
		try (RocksIterator iterator = (snapshot != null ? db.newIterator(cfh, new ReadOptions().setSnapshot(snapshot))
				: db.newIterator(cfh))) {
			iterator.seekToFirst();
			while (iterator.isValid()) {
				var combinedKey = iterator.key();
				var key1 = getKey1(combinedKey);
				var key2 = getKey2(combinedKey);

				var result = consumer.acceptCancellable(key1, key2, iterator.value());
				if (result.isCancelled()) {
					return ConsumerResult.cancelNext();
				}

				iterator.next();
			}
			return ConsumerResult.result();
		}
	}

	@Override
	public ConsumerResult forEach(@Nullable LLSnapshot snapshot, int parallelism, CancellableBiConsumer<byte[], UnmodifiableIterableMap<byte[], byte[]>> consumer) {
		return forEach_(consumer, snapshot == null ? null : snapshotResolver.apply(snapshot), parallelism);
	}

	//todo: implement parallel execution
	private ConsumerResult forEach_(CancellableBiConsumer<byte[], UnmodifiableIterableMap<byte[], byte[]>> consumer, @Nullable Snapshot snapshot, int parallelism) {
		try (RocksIterator iterator = (snapshot != null ? db.newIterator(cfh, new ReadOptions().setSnapshot(snapshot))
				: db.newIterator(cfh))) {
			iterator.seekToFirst();
			byte[] currentKey1 = null;
			// only append or iterate on this object! byte[].equals() and hash is not trustworthy!
			List<byte[]> key2Keys = null;
			// only append or iterate on this object! byte[].equals() and hash is not trustworthy!
			List<byte[]> key2Values = null;
			while (iterator.isValid()) {
				var combinedKey = iterator.key();
				var key1 = getKey1(combinedKey);

				if (currentKey1 == null || !Arrays.equals(currentKey1, key1)) {
					if (currentKey1 != null && !key2Values.isEmpty()) {
						var result = consumer.acceptCancellable(currentKey1, UnmodifiableIterableMap.of(key2Keys.toArray(byte[][]::new), key2Values.toArray(byte[][]::new)));
						if (result.isCancelled()) {
							return ConsumerResult.cancelNext();
						}
					}
					currentKey1 = key1;
					key2Keys = new ArrayList<>();
					key2Values = new ArrayList<>();
				}

				key2Keys.add(getKey2(combinedKey));
				key2Values.add(iterator.value());

				iterator.next();
			}
			if (currentKey1 != null && !key2Values.isEmpty()) {
				var result = consumer.acceptCancellable(currentKey1, UnmodifiableIterableMap.of(key2Keys.toArray(byte[][]::new), key2Values.toArray(byte[][]::new)));
				if (result.isCancelled()) {
					return ConsumerResult.cancelNext();
				}
			}
			return ConsumerResult.result();
		}
	}

	@Override
	public ConsumerResult forEach(@Nullable LLSnapshot snapshot, int parallelism, byte[] key, CancellableBiConsumer<byte[], byte[]> consumer) {
		return forEach_(key, consumer, snapshot == null ? null : snapshotResolver.apply(snapshot), parallelism);
	}

	//todo: implement parallel execution
	private ConsumerResult forEach_(byte[] key1, CancellableBiConsumer<byte[], byte[]> consumer, @Nullable Snapshot snapshot, int parallelism) {
		try (RocksIterator iterator = (snapshot != null ? db.newIterator(cfh, new ReadOptions().setSnapshot(snapshot))
				: db.newIterator(cfh))) {
			iterator.seek(getStartSeekKey(key1));
			while (iterator.isValid()) {
				byte[] combinedKey = iterator.key();

				if (!isSubKey(key1, combinedKey)) {
					// The key is outside of key1: exit from the iteration
					break;
				}

				byte[] key2 = getKey2(combinedKey);
				byte[] value2 = iterator.value();
				var result = consumer.acceptCancellable(key2, value2);
				if (result.isCancelled()) {
					return ConsumerResult.cancelNext();
				}

				iterator.next();
			}
			return ConsumerResult.result();
		}
	}

	//todo: implement parallel execution
	//todo: implement replaceKeys = false optimization (like in LLLocalDictionary), check if it's feasible
	@Override
	public ConsumerResult replaceAll(int parallelism, boolean replaceKeys, CancellableTriFunction<byte[], byte[], byte[], ImmutableTriple<byte[], byte[], byte[]>> consumer) throws IOException {
		var snapshot = db.getSnapshot();
		try {
			try (RocksIterator iter = db.newIterator(cfh, new ReadOptions().setSnapshot(snapshot));
					CappedWriteBatch writeBatch = new CappedWriteBatch(db, LLLocalDictionary.CAPPED_WRITE_BATCH_CAP, LLLocalDictionary.RESERVED_WRITE_BATCH_SIZE, LLLocalDictionary.MAX_WRITE_BATCH_SIZE, LLLocalDictionary.BATCH_WRITE_OPTIONS)) {

				iter.seekToFirst();

				while (iter.isValid()) {

					writeBatch.delete(cfh, iter.key());

					iter.next();
				}

				iter.seekToFirst();

				while (iter.isValid()) {
					var combinedKey = iter.key();
					var key1 = getKey1(combinedKey);
					var key2 = getKey2(combinedKey);

					var result = consumer.applyCancellable(key1, key2, iter.value());
					if (result.getValue().getLeft().length != key1Size) {
						throw new IndexOutOfBoundsException(result.getValue().getLeft().length, key1Size, key1Size);
					}
					if (result.getValue().getMiddle().length != key2Size) {
						throw new IndexOutOfBoundsException(result.getValue().getMiddle().length, key2Size, key2Size);
					}

					writeBatch.put(cfh, getCombinedKey(result.getValue().getLeft(), result.getValue().getMiddle()), result.getValue().getRight());

					if (result.isCancelled()) {
						// Cancels and discards the write batch
						writeBatch.clear();
						return ConsumerResult.cancelNext();
					}

					iter.next();
				}

				writeBatch.writeToDbAndClose();

				return ConsumerResult.result();
			}
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			db.releaseSnapshot(snapshot);
			snapshot.close();
		}
	}

	//todo: implement parallel execution
	//todo: implement replaceKeys = false optimization (like in LLLocalDictionary), check if it's feasible
	@Override
	public ConsumerResult replaceAll(int parallelism, boolean replaceKeys, CancellableBiFunction<byte[], UnmodifiableIterableMap<byte[], byte[]>, Entry<byte[], UnmodifiableMap<Bytes, byte[]>>> consumer)
			throws IOException {
		try {
			var snapshot = db.getSnapshot();
			try (RocksIterator iter = db.newIterator(cfh, new ReadOptions().setSnapshot(snapshot));
					CappedWriteBatch writeBatch = new CappedWriteBatch(db, LLLocalDictionary.CAPPED_WRITE_BATCH_CAP, LLLocalDictionary.RESERVED_WRITE_BATCH_SIZE, LLLocalDictionary.MAX_WRITE_BATCH_SIZE, LLLocalDictionary.BATCH_WRITE_OPTIONS)) {

				iter.seekToFirst();

				while (iter.isValid()) {

					writeBatch.delete(cfh, iter.key());

					iter.next();
				}

				iter.seekToFirst();

				byte[] currentKey1 = null;
				// only append or iterate on this object! byte[].equals() and hash is not trustworthy!
				ObjectArrayList<byte[]> key2Keys = null;
				// only append or iterate on this object! byte[].equals() and hash is not trustworthy!
				ObjectArrayList<byte[]> key2Values = null;
				while (iter.isValid()) {
					var combinedKey = iter.key();
					var key1 = getKey1(combinedKey);

					if (currentKey1 == null || !Arrays.equals(currentKey1, key1)) {
						if (currentKey1 != null && !key2Values.isEmpty()) {
							var result = replaceAll_(writeBatch,
									currentKey1,
									key2Keys.toArray(byte[][]::new),
									key2Values.toArray(byte[][]::new),
									consumer
							);

							if (result.isCancelled()) {
								// Cancels and discards the write batch
								writeBatch.clear();
								return ConsumerResult.cancelNext();
							}
						}
						currentKey1 = key1;
						key2Keys = new ObjectArrayList<>();
						key2Values = new ObjectArrayList<>();
					}

					key2Keys.add(getKey2(combinedKey));
					key2Values.add(iter.value());

					iter.next();
				}
				if (currentKey1 != null && !key2Values.isEmpty()) {
					var result = replaceAll_(writeBatch,
							currentKey1,
							key2Keys.toArray(byte[][]::new),
							key2Values.toArray(byte[][]::new),
							consumer
					);

					if (result.isCancelled()) {
						// Cancels and discards the write batch
						writeBatch.clear();
						return ConsumerResult.cancelNext();
					}
				}

				writeBatch.writeToDbAndClose();

				return ConsumerResult.result();
			} finally {
				db.releaseSnapshot(snapshot);
				snapshot.close();
			}
		} catch (RocksDBException exception) {
			throw new IOException(exception);
		}
	}

	private ConsumerResult replaceAll_(WriteBatchInterface writeBatch,
			byte[] key1,
			byte[][] key2Keys,
			byte[][] key2Values,
			CancellableBiFunction<byte[], UnmodifiableIterableMap<byte[], byte[]>, Entry<byte[], UnmodifiableMap<Bytes, byte[]>>> consumer)
			throws RocksDBException {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		var previousValues = UnmodifiableMap.of(key2Keys, key2Values);
		var result = consumer.applyCancellable(key1, previousValues);

		var resultKey1 = result.getValue().getKey();
		if (resultKey1.length != key1Size) {
			throw new IndexOutOfBoundsException(resultKey1.length, key1Size, key1Size);
		}
		var resultValues = result.getValue().getValue();

		var mapIterator = resultValues.fastIterator();
		while (mapIterator.hasNext()) {
			var mapEntry = mapIterator.next();
			var key2 = mapEntry.getKey();
			if (key2.data.length != key2Size) {
				throw new IndexOutOfBoundsException(key2.data.length, key2Size, key2Size);
			}

			var value2 = mapEntry.getValue();
			writeBatch.put(cfh, getCombinedKey(key1, key2.data), value2);

			if (result.isCancelled()) {
				// Cancels and discards the write batch
				writeBatch.clear();
				return ConsumerResult.cancelNext();
			}
		}
		return ConsumerResult.result();
	}

	//todo: implement parallel execution
	//todo: implement replaceKeys = false optimization (like in LLLocalDictionary), check if it's feasible
	@Override
	public ConsumerResult replaceAll(int parallelism, boolean replaceKeys, byte[] key1, CancellableBiFunction<byte[], byte[], Entry<byte[], byte[]>> consumer) throws IOException {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		try {
			var snapshot = db.getSnapshot();
			try (RocksIterator iter = db.newIterator(cfh, new ReadOptions().setSnapshot(snapshot));
					CappedWriteBatch writeBatch = new CappedWriteBatch(db, LLLocalDictionary.CAPPED_WRITE_BATCH_CAP, LLLocalDictionary.RESERVED_WRITE_BATCH_SIZE, LLLocalDictionary.MAX_WRITE_BATCH_SIZE, LLLocalDictionary.BATCH_WRITE_OPTIONS)) {

				iter.seek(getStartSeekKey(key1));

				while (iter.isValid()) {
					byte[] combinedKey = iter.key();

					if (!isSubKey(key1, combinedKey)) {
						// The key is outside of key1: exit from the iteration
						break;
					}

					writeBatch.delete(cfh, combinedKey);

					iter.next();
				}

				iter.seek(getStartSeekKey(key1));

				while (iter.isValid()) {
					byte[] combinedKey = iter.key();

					if (!isSubKey(key1, combinedKey)) {
						// The key is outside of key1: exit from the iteration
						break;
					}

					byte[] key2 = getKey2(combinedKey);
					byte[] value2 = iter.value();

					var result = consumer.applyCancellable(key2, value2);
					if (result.getValue().getKey().length != key2Size) {
						throw new IndexOutOfBoundsException(result.getValue().getKey().length, key2Size, key2Size);
					}

					writeBatch.put(cfh, result.getValue().getKey(), result.getValue().getValue());

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
				snapshot.close();
			}
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	// This method is exactly the same of LLLocalDictionary. Remember to keep the code equal
	@Override
	public void clear() throws IOException {
		try {
			List<byte[]> ranges = new ArrayList<>();
			byte[] firstKey = null;
			byte[] lastKey = null;
			boolean empty = false;
			while (!empty) {
				// retrieve the range extremities
				try (RocksIterator iter = db.newIterator(cfh)) {
					iter.seekToFirst();
					if (iter.isValid()) {
						firstKey = iter.key();
						iter.seekToLast();
						lastKey = iter.key();
						ranges.add(firstKey);
						ranges.add(lastKey);
					} else {
						empty = true;
					}
				}

				if (!empty) {
					if (Arrays.equals(firstKey, lastKey)) {
						// Delete single key
						db.delete(cfh, lastKey);
					} else {
						// Delete all
						db.deleteRange(cfh, firstKey, lastKey);
						// Delete the end because it's not included in the deleteRange domain
						db.delete(cfh, lastKey);
					}
				}
			}

			// Delete files related
			db.deleteFilesInRanges(cfh, ranges, true);

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
	public Optional<UnmodifiableIterableMap<byte[], byte[]>> clear(byte[] key1, LLDictionaryResultType resultType)
			throws IOException {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		try {
			Optional<UnmodifiableIterableMap<byte[], byte[]>> result;
			switch (resultType) {
				case PREVIOUS_VALUE:
					List<byte[]> keys = new ArrayList<>();
					List<byte[]> values = new ArrayList<>();
					try (RocksIterator iter = db.newIterator(cfh)) {
						iter.seek(getStartSeekKey(key1));
						while (iter.isValid()) {
							var combinedKey = iter.key();

							if (!isSubKey(key1, combinedKey)) {
								break;
							}

							keys.add(getKey2(combinedKey));
							values.add(iter.value());
						}
					}
					result = Optional.of(UnmodifiableIterableMap.of(keys.toArray(byte[][]::new), values.toArray(byte[][]::new)));
					break;
				case VALUE_CHANGED:
					if (isEmpty(null, key1)) {
						result = Optional.empty();
					} else {
						result = Optional.of(UnmodifiableIterableMap.of(NO_DATA_MAP, NO_DATA_MAP));
					}
					break;
				case VOID:
				default:
					result = Optional.empty();
					break;
			}
			db.deleteRange(cfh, getStartSeekKey(key1), getEndSeekKey(key1));
			return result;
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public long size(@Nullable LLSnapshot snapshot, boolean fast) {
		return fast ? fastSize(snapshot) : exactSize(snapshot);
	}

	public long fastSize(@Nullable LLSnapshot snapshot) {
		try {
			if (snapshot != null) {
				return this.exactSize(snapshot);
			}
			return db.getLongProperty(cfh, "rocksdb.estimate-num-keys");
		} catch (RocksDBException e) {
			e.printStackTrace();
			return 0;
		}
	}

	public long exactSize(@Nullable LLSnapshot snapshot) {
		long count = 0;
		byte[] currentKey1 = null;
		try (RocksIterator iter = db.newIterator(cfh, resolveSnapshot(snapshot))) {
			iter.seekToFirst();
			while (iter.isValid()) {
				byte[] combinedKey = iter.key();

				if (!isSubKey(currentKey1, combinedKey)) {
					count++;
					currentKey1 = getKey1(combinedKey);
				}
				iter.next();
			}
			return count;
		}
	}

	@Override
	public long exactSize(@Nullable LLSnapshot snapshot, byte[] key1) {
		if (key1.length != key1Size) {
			throw new IndexOutOfBoundsException(key1.length, key1Size, key1Size);
		}
		long count = 0;
		try (RocksIterator iterator = db.newIterator(cfh, resolveSnapshot(snapshot))) {
			iterator.seek(getStartSeekKey(key1));
			while (iterator.isValid()) {
				byte[] combinedKey = iterator.key();

				if (!isSubKey(key1, combinedKey)) {
					// The key is outside of key1: exit from the iteration
					break;
				}

				count++;
				iterator.next();
			}
		}
		return count;
	}
}
