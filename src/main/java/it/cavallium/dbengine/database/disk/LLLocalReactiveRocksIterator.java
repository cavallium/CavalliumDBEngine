package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.disk.LLLocalDictionary.getRocksIterator;

import it.cavallium.dbengine.database.LLRange;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksMutableObject;
import reactor.core.publisher.Flux;

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

	public Flux<T> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					if (!range.hasMin() || !range.hasMax()) {
						readOptions.setReadaheadSize(2 * 1024 * 1024);
						readOptions.setFillCache(false);
					}
					return getRocksIterator(readOptions, range, db, cfh);
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
