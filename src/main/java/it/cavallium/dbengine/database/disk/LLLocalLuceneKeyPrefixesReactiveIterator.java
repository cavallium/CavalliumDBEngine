package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import java.util.Arrays;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Slice;
import reactor.core.publisher.Flux;

public class LLLocalLuceneKeyPrefixesReactiveIterator {

	private static final byte[] EMPTY = new byte[0];

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final int prefixLength;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final String debugName;

	public LLLocalLuceneKeyPrefixesReactiveIterator(RocksDB db,
			ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			String debugName) {
		this.db = db;
		this.cfh = cfh;
		this.prefixLength = prefixLength;
		this.range = range;
		this.readOptions = readOptions;
		this.debugName = debugName;
	}


	@SuppressWarnings("Convert2MethodRef")
	public Flux<byte[]> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					readOptions.setFillCache(range.hasMin() && range.hasMax());
					if (range.hasMin()) {
						readOptions.setIterateLowerBound(new Slice(range.getMin()));
					}
					if (range.hasMax()) {
						readOptions.setIterateUpperBound(new Slice(range.getMax()));
					}
					readOptions.setPrefixSameAsStart(true);
					var rocksIterator = db.newIterator(cfh, readOptions);
					if (range.hasMin()) {
						rocksIterator.seek(range.getMin());
					} else {
						rocksIterator.seekToFirst();
					}
					return rocksIterator;
				}, (rocksIterator, sink) -> {
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
					return rocksIterator;
				}, rocksIterator1 -> rocksIterator1.close());
	}
}
