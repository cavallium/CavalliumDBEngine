package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import java.util.Optional;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksMutableObject;
import org.rocksdb.Slice;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

public abstract class LLLocalReactiveRocksIterator<T> {

	private static final byte[] EMPTY = new byte[0];

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final boolean readValues;

	public LLLocalReactiveRocksIterator(RocksDB db,
			ColumnFamilyHandle cfh,
			LLRange range,
			ReadOptions readOptions,
			boolean readValues) {
		this.db = db;
		this.cfh = cfh;
		this.range = range;
		this.readOptions = readOptions;
		this.readValues = readValues;
	}

	@SuppressWarnings("Convert2MethodRef")
	public Flux<T> flux() {
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
					if (rocksIterator.isValid()) {
						byte[] key = rocksIterator.key();
						byte[] value = readValues ? rocksIterator.value() : EMPTY;
						rocksIterator.next();
						sink.next(getEntry(key, value));
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
