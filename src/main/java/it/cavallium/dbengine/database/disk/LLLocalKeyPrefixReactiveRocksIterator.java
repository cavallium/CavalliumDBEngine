package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import java.util.Arrays;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksMutableObject;
import reactor.core.publisher.Flux;

public class LLLocalKeyPrefixReactiveRocksIterator {

	private static final byte[] EMPTY = new byte[0];

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final int prefixLength;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final boolean canFillCache;
	private final String debugName;

	public LLLocalKeyPrefixReactiveRocksIterator(RocksDB db,
			ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			boolean canFillCache,
			String debugName) {
		this.db = db;
		this.cfh = cfh;
		this.prefixLength = prefixLength;
		this.range = range;
		this.readOptions = readOptions;
		this.canFillCache = canFillCache;
		this.debugName = debugName;
	}


	public Flux<byte[]> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					if (!range.hasMin() || !range.hasMax()) {
						//readOptions.setReadaheadSize(2 * 1024 * 1024);
						readOptions.setFillCache(canFillCache);
					}
					return LLLocalDictionary.getRocksIterator(readOptions, range, db, cfh);
				}, (tuple, sink) -> {
					var rocksIterator = tuple.getT1();
					byte[] firstGroupKey = null;

					while (rocksIterator.isValid()) {
						byte[] key = rocksIterator.key();
						if (firstGroupKey == null) {
							firstGroupKey = key;
						} else if (!Arrays.equals(firstGroupKey, 0, prefixLength, key, 0, prefixLength)) {
							break;
						}
						rocksIterator.next();
					}
					if (firstGroupKey != null) {
						var groupKeyPrefix = Arrays.copyOf(firstGroupKey, prefixLength);
						sink.next(groupKeyPrefix);
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

}
