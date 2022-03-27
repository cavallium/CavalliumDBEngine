package it.cavallium.dbengine.database.disk;

import org.rocksdb.AbstractSlice;
import org.rocksdb.AbstractTableFilter;
import org.rocksdb.ReadOptions;
import org.rocksdb.ReadTier;
import org.rocksdb.Snapshot;
import org.warp.commonutils.range.UnmodifiableRange;

public class UnmodifiableReadOptions extends ReadOptions {

	public UnmodifiableReadOptions() {

	}

	public UnmodifiableReadOptions(ReadOptions readOptions) {
		super(readOptions);
	}

	@Override
	public ReadOptions setBackgroundPurgeOnIteratorCleanup(boolean b) {
		throw uoe();
	}

	@Override
	public ReadOptions setFillCache(boolean b) {
		throw uoe();
	}

	@Override
	public ReadOptions setSnapshot(Snapshot snapshot) {
		throw uoe();
	}

	@Override
	public ReadOptions setReadTier(ReadTier readTier) {
		throw uoe();
	}

	@Override
	public ReadOptions setTailing(boolean b) {
		throw uoe();
	}

	@Override
	public ReadOptions setVerifyChecksums(boolean b) {
		throw uoe();
	}

	@Override
	public ReadOptions setManaged(boolean b) {
		throw uoe();
	}

	@Override
	public ReadOptions setTotalOrderSeek(boolean b) {
		throw uoe();
	}

	@Override
	public ReadOptions setPrefixSameAsStart(boolean b) {
		throw uoe();
	}

	@Override
	public ReadOptions setPinData(boolean b) {
		throw uoe();
	}

	@Override
	public ReadOptions setReadaheadSize(long l) {
		throw uoe();
	}

	@Override
	public ReadOptions setMaxSkippableInternalKeys(long l) {
		throw uoe();
	}

	@Override
	public ReadOptions setIgnoreRangeDeletions(boolean b) {
		throw uoe();
	}

	@Override
	public ReadOptions setIterateLowerBound(AbstractSlice<?> abstractSlice) {
		throw uoe();
	}

	@Override
	public ReadOptions setIterateUpperBound(AbstractSlice<?> abstractSlice) {
		throw uoe();
	}

	@Override
	public ReadOptions setTableFilter(AbstractTableFilter abstractTableFilter) {
		throw uoe();
	}

	private UnsupportedOperationException uoe() {
		return new UnsupportedOperationException("Unmodifiable read options");
	}
}
