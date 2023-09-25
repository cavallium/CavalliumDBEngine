package it.cavallium.dbengine.database.disk.rocksdb;

import it.cavallium.dbengine.database.disk.IteratorMetrics;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.Arrays;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.ReadOptions;
import org.rocksdb.ReadTier;
import org.rocksdb.RocksDB;
import org.rocksdb.Snapshot;

public final class LLReadOptions extends SimpleResource {

	private final ReadOptions val;
	private LLSlice itLowerBoundRef;
	private boolean itLowerBoundRefOwned;
	private LLSlice itUpperBoundRef;
	private boolean itUpperBoundRefOwned;
	private Snapshot snapshot;

	public LLReadOptions copy() {

		if (this.val.timestamp() != null) {
			throw new IllegalStateException("Unsupported copy of timestamped read options");
		}

		if (this.val.iterStartTs() != null) {
			throw new IllegalStateException("Unsupported copy of read options with non-null iterStartTs property");
		}

		ReadOptions newVal = new ReadOptions(this.val);

		var ro = new LLReadOptions(newVal);
		if (this.val.iterateLowerBound() != null) {
			ro.setIterateLowerBound(this.val.iterateLowerBound().data());
		}
		if (this.val.iterateUpperBound() != null) {
			ro.setIterateUpperBound(this.val.iterateUpperBound().data());
		}
		ro.snapshot = this.snapshot;
		return ro;
	}

	public LLReadOptions(ReadOptions val) {
		super(val::close);
		this.val = val;
	}

	public LLReadOptions() {
		this(new ReadOptions());
	}

	@Override
	protected void onClose() {
		val.close();

		if (this.itUpperBoundRefOwned && this.itUpperBoundRef != null) {
			this.itUpperBoundRef.close();
		}
		this.itUpperBoundRef = null;

		if (this.itLowerBoundRefOwned && this.itLowerBoundRef != null) {
			this.itLowerBoundRef.close();
		}
		this.itLowerBoundRef = null;

		snapshot = null;
	}


	public void setIterateLowerBound(byte[] slice) {
		setIterateLowerBound(slice != null ? LLSlice.copyOf(slice) : null, true);
	}
	/**
	 * @param move if true, the read options will close the slice when they are closed
	 */
	public void setIterateLowerBound(LLSlice slice, boolean move) {
		val.setIterateLowerBound(slice != null ? slice.getSliceUnsafe() : null);

		// Close the previous owned value, if present
		if (this.itLowerBoundRefOwned && this.itLowerBoundRef != null) {
			this.itLowerBoundRef.close();
		}

		this.itLowerBoundRef = slice;
		this.itLowerBoundRefOwned = move;
	}

	public void setIterateUpperBound(byte[] slice) {
		setIterateUpperBound(slice != null ? LLSlice.copyOf(slice) : null, true);
	}

	/**
	 * @param move if true, the read options will close the slice when they are closed
	 */
	public void setIterateUpperBound(LLSlice slice, boolean move) {
		val.setIterateUpperBound(slice != null ? slice.getSliceUnsafe() : null);

		// Close the previous owned value, if present
		if (this.itUpperBoundRefOwned && this.itUpperBoundRef != null) {
			this.itUpperBoundRef.close();
		}

		this.itUpperBoundRef = slice;
		this.itUpperBoundRefOwned = move;
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

	public boolean hasIterateLowerBound() {
		return val.iterateLowerBound() != null;
	}

	public boolean hasIterateUpperBound() {
		return val.iterateUpperBound() != null;
	}
}
