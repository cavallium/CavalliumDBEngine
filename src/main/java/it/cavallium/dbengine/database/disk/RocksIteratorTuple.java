package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.SafeCloseable;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;

public record RocksIteratorTuple(List<RocksObject> refs, @NotNull RocksDBIterator iterator,
																 @NotNull ReleasableSlice sliceMin, @NotNull ReleasableSlice sliceMax,
																 @NotNull SafeCloseable seekTo) implements
		SafeCloseable {

	@Override
	public void close() {
		for (RocksObject rocksObject : refs) {
			if (rocksObject instanceof UnreleasableReadOptions) {
				continue;
			}
			if (rocksObject instanceof UnreleasableWriteOptions) {
				continue;
			}
			rocksObject.close();
		}
		iterator.close();
		sliceMin.close();
		sliceMax.close();
		seekTo.close();
	}
}
