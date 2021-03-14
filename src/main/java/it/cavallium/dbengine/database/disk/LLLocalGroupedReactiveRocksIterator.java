package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksMutableObject;
import org.rocksdb.Slice;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

public abstract class LLLocalGroupedReactiveRocksIterator<T> {

	private static final byte[] EMPTY = new byte[0];

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final int prefixLength;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final boolean readValues;
	private final String debugName;

	public LLLocalGroupedReactiveRocksIterator(RocksDB db,
			ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			boolean readValues,
			String debugName) {
		this.db = db;
		this.cfh = cfh;
		this.prefixLength = prefixLength;
		this.range = range;
		this.readOptions = readOptions;
		this.readValues = readValues;
		this.debugName = debugName;
	}


	@SuppressWarnings("Convert2MethodRef")
	public Flux<List<T>> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					readOptions.setFillCache(range.hasMin() && range.hasMax());
					Slice sliceMin;
					Slice sliceMax;
					if (range.hasMin()) {
						sliceMin = new Slice(range.getMin());
						readOptions.setIterateLowerBound(sliceMin);
					} else {
						sliceMin = null;
					}
					if (range.hasMax()) {
						sliceMax = new Slice(range.getMax());
						readOptions.setIterateUpperBound(sliceMax);
					} else {
						sliceMax = null;
					}
					var rocksIterator = db.newIterator(cfh, readOptions);
					if (!LLLocalDictionary.PREFER_SEEK_TO_FIRST && range.hasMin()) {
						rocksIterator.seek(range.getMin());
					} else {
						rocksIterator.seekToFirst();
					}
					return Tuples.of(rocksIterator, Optional.ofNullable(sliceMin), Optional.ofNullable(sliceMax));
				}, (tuple, sink) -> {
					var rocksIterator = tuple.getT1();
					ObjectArrayList<T> values = new ObjectArrayList<>();
					byte[] firstGroupKey = null;

					while (rocksIterator.isValid()) {
						byte[] key = rocksIterator.key();
						if (firstGroupKey == null) {
							firstGroupKey = key;
						} else if (!Arrays.equals(firstGroupKey, 0, prefixLength, key, 0, prefixLength)) {
							break;
						}
						byte[] value = readValues ? rocksIterator.value() : EMPTY;
						rocksIterator.next();
						values.add(getEntry(key, value));
					}
					if (!values.isEmpty()) {
						sink.next(values);
					} else {
						sink.complete();
					}
					return tuple;
				}, tuple -> {
					var rocksIterator = tuple.getT1();
					rocksIterator.close();
					tuple.getT2().ifPresent(RocksMutableObject::close);
					tuple.getT3().ifPresent(RocksMutableObject::close);
				});
	}

	public abstract T getEntry(byte[] key, byte[] value);
}
