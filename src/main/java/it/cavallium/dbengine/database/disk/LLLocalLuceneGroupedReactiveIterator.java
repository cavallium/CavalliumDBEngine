package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Arrays;
import java.util.List;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Slice;
import reactor.core.publisher.Flux;

public abstract class LLLocalLuceneGroupedReactiveIterator<T> {

	private static final byte[] EMPTY = new byte[0];

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final int prefixLength;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final boolean readValues;
	private final String debugName;

	public LLLocalLuceneGroupedReactiveIterator(RocksDB db,
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
					synchronized (this) {
						var readOptions = new ReadOptions(this.readOptions);
						readOptions.setFillCache(range.hasMin() && range.hasMax());
						if (range.hasMin()) {
							readOptions.setIterateLowerBound(new Slice(range.getMin()));
						}
						if (range.hasMax()) {
							readOptions.setIterateUpperBound(new Slice(range.getMax()));
						}
						var rocksIterator = db.newIterator(cfh, readOptions);
						if (!LLLocalDictionary.PREFER_ALWAYS_SEEK_TO_FIRST && range.hasMin()) {
							rocksIterator.seek(range.getMin());
						} else {
							rocksIterator.seekToFirst();
						}
						return rocksIterator;
					}
				}, (rocksIterator, sink) -> {
					synchronized (this) {
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
						return rocksIterator;
					}
				}, rocksIterator1 -> {
					synchronized (this) {
						rocksIterator1.close();
					}
				});
	}

	public abstract T getEntry(byte[] key, byte[] value);
}
