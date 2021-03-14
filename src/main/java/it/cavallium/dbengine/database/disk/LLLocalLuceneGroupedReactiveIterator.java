package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Arrays;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Slice;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

public abstract class LLLocalLuceneGroupedReactiveIterator<T> extends Flux<List<T>> {

	private static final byte[] EMPTY = new byte[0];

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final int prefixLength;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final boolean readValues;

	public LLLocalLuceneGroupedReactiveIterator(RocksDB db,
			ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			boolean readValues) {
		this.db = db;
		this.cfh = cfh;
		this.prefixLength = prefixLength;
		this.range = range;
		this.readOptions = readOptions;
		this.readValues = readValues;
	}

	@Override
	public void subscribe(@NotNull CoreSubscriber<? super List<T>> actual) {
		Flux<List<T>> flux = Flux
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
				}, tuple -> {});
		flux.subscribe(actual);
	}

	public abstract T getEntry(byte[] key, byte[] value);
}
