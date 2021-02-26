package it.cavallium.dbengine.database;

import java.util.Arrays;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import reactor.core.scheduler.Scheduler;

public abstract class BoundedRocksFluxIterable<T> extends BlockingFluxIterable<T> {

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	protected final LLRange range;

	protected RocksIterator rocksIterator;
	protected ReadOptions readOptions;

	public BoundedRocksFluxIterable(Scheduler scheduler,
			RocksDB db,
			ColumnFamilyHandle cfh,
			LLRange range) {
		super(scheduler);
		this.db = db;
		this.cfh = cfh;
		this.range = range;
	}

	@Override
	public void onStartup() {
		readOptions = this.getReadOptions();
		rocksIterator = db.newIterator(cfh, readOptions);
		if (range.hasMin()) {
			rocksIterator.seek(range.getMin());
		} else {
			rocksIterator.seekToFirst();
		}
	}

	@Override
	public void onTerminate() {
		if (rocksIterator != null) {
			rocksIterator.close();
		}
	}

	@Nullable
	@Override
	public T onNext() {
		if (!rocksIterator.isValid()) {
			return null;
		}
		byte[] key = rocksIterator.key();
		if (range.hasMax() && Arrays.compareUnsigned(key, range.getMax()) > 0) {
			return null;
		}
		rocksIterator.next();
		return this.transformEntry(key);
	}

	protected abstract ReadOptions getReadOptions();

	protected abstract T transformEntry(byte[] key);

	protected byte[] getValue() {
		return rocksIterator.value();
	}
}
