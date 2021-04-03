package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.disk.LLLocalDictionary.getRocksIterator;

import it.cavallium.dbengine.database.LLRange;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Arrays;
import java.util.List;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksMutableObject;
import reactor.core.publisher.Flux;

public abstract class LLLocalGroupedReactiveRocksIterator<T> {

	private static final byte[] EMPTY = new byte[0];

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final int prefixLength;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final boolean canFillCache;
	private final boolean readValues;

	public LLLocalGroupedReactiveRocksIterator(RocksDB db,
			ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			boolean canFillCache,
			boolean readValues) {
		this.db = db;
		this.cfh = cfh;
		this.prefixLength = prefixLength;
		this.range = range;
		this.readOptions = readOptions;
		this.canFillCache = canFillCache;
		this.readValues = readValues;
	}


	public Flux<List<T>> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					readOptions.setFillCache(canFillCache && range.hasMin() && range.hasMax());
					return getRocksIterator(readOptions, range, db, cfh);
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
