package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Slice;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

public abstract class LLLocalLuceneReactiveIterator<T> extends Flux<T> {

	private static final byte[] EMPTY = new byte[0];

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final boolean readValues;

	public LLLocalLuceneReactiveIterator(RocksDB db,
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

	@Override
	public void subscribe(@NotNull CoreSubscriber<? super T> actual) {
		Flux<T> flux = Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					readOptions.setFillCache(range.hasMin() && range.hasMax());
					if (range.hasMin()) {
						readOptions.setIterateLowerBound(new Slice(range.getMin()));
					}
					if (range.hasMax()) {
						readOptions.setIterateUpperBound(new Slice(range.getMax()));
					}
					var rocksIterator = db.newIterator(cfh, readOptions);
					if (range.hasMin()) {
						rocksIterator.seek(range.getMin());
					} else {
						rocksIterator.seekToFirst();
					}
					return rocksIterator;
				}, (rocksIterator, sink) -> {
					if (rocksIterator.isValid()) {
						byte[] key = rocksIterator.key();
						byte[] value = readValues ? rocksIterator.value() : EMPTY;
						rocksIterator.next();
						sink.next(getEntry(key, value));
					} else {
						sink.complete();
					}
					return rocksIterator;
				}, AbstractImmutableNativeReference::close);
		flux.subscribe(actual);
	}

	public abstract T getEntry(byte[] key, byte[] value);
}
