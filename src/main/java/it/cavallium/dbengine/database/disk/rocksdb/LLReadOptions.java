package it.cavallium.dbengine.database.disk.rocksdb;

import it.cavallium.dbengine.database.disk.IteratorMetrics;
import it.cavallium.dbengine.utils.SimpleResource;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Snapshot;

public final class LLReadOptions extends SimpleResource {

	private final ReadOptions val;
	private LLSlice itLowerBoundRef;
	private LLSlice itUpperBoundRef;
	private Snapshot snapshot;

	public LLReadOptions(ReadOptions val) {
		super(val::close);
		this.val = val;
	}

	public LLReadOptions() {
		this(new ReadOptions());
	}

	public LLReadOptions copy() {
		var ro = new LLReadOptions(new ReadOptions(this.val));
		ro.itUpperBoundRef = this.itUpperBoundRef;
		ro.itLowerBoundRef = this.itLowerBoundRef;
		ro.snapshot = this.snapshot;
		return ro;
	}

	@Override
	protected void onClose() {
		val.close();
		itLowerBoundRef = null;
		itUpperBoundRef = null;
		snapshot = null;
	}

	public void setIterateLowerBound(LLSlice slice) {
		val.setIterateLowerBound(slice.getSliceUnsafe());
		itLowerBoundRef = slice;
	}

	public void setIterateUpperBound(LLSlice slice) {
		val.setIterateUpperBound(slice.getSliceUnsafe());
		itUpperBoundRef = slice;
	}

	public long readaheadSize() {
		return val.readaheadSize();
	}

	public void setReadaheadSize(long readaheadSize) {
		val.setReadaheadSize(readaheadSize);
	}

	public RocksIteratorObj newIterator(RocksDB db, ColumnFamilyHandle cfh, IteratorMetrics iteratorMetrics) {
		return new RocksIteratorObj(db.newIterator(cfh, val), this, iteratorMetrics);
	}

	public void setFillCache(boolean fillCache) {
		val.setFillCache(fillCache);
	}

	public void setVerifyChecksums(boolean verifyChecksums) {
		val.setVerifyChecksums(verifyChecksums);
	}

	public ReadOptions getUnsafe() {
		return val;
	}

	public LLReadOptions setSnapshot(Snapshot snapshot) {
		val.setSnapshot(snapshot);
		this.snapshot = snapshot;
		return this;
	}

	public void setIgnoreRangeDeletions(boolean ignoreRangeDeletions) {
		val.setIgnoreRangeDeletions(ignoreRangeDeletions);
	}

	public Snapshot snapshot() {
		return val.snapshot();
	}
}
