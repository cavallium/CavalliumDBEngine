package it.cavallium.dbengine.database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

public abstract class BoundedGroupedRocksFluxIterable<T> extends BlockingFluxIterable<List<T>> {

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	protected final LLRange range;
	private final int prefixLength;

	protected RocksIterator rocksIterator;
	protected ReadOptions readOptions;

	public BoundedGroupedRocksFluxIterable(RocksDB db,
			ColumnFamilyHandle cfh,
			LLRange range,
			int prefixLength) {
		super("bounded-grouped-rocksdb");
		this.db = db;
		this.cfh = cfh;
		this.range = range;
		this.prefixLength = prefixLength;
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
	public List<T> onNext() {
		byte[] firstGroupKey = null;
		List<T> currentGroupValues = new ArrayList<>();
		while (rocksIterator.isValid()) {
			byte[] key = rocksIterator.key();
			if (firstGroupKey == null) { // Fix first value
				firstGroupKey = key;
			}
			if (range.hasMax() && Arrays.compareUnsigned(key, range.getMax()) > 0) {
				rocksIterator.next();
				break;
			} else {
				List<T> result = null;

				if (Arrays.equals(firstGroupKey, 0, prefixLength, key, 0, prefixLength)) {
					currentGroupValues.add(transformEntry(key));
				} else {
					if (!currentGroupValues.isEmpty()) {
						result = currentGroupValues;
					}
					firstGroupKey = key;
					currentGroupValues = new ArrayList<>();
				}
				if (result != null) {
					rocksIterator.next();
					return result;
				} else {
					rocksIterator.next();
				}
			}
		}
		if (!currentGroupValues.isEmpty()) {
			return currentGroupValues;
		}
		return null;
	}

	protected abstract ReadOptions getReadOptions();

	protected abstract T transformEntry(byte[] key);

	protected byte[] getValue() {
		return rocksIterator.value();
	}
}
