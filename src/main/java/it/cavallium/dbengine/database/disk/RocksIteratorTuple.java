package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.SafeCloseable;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;

public record RocksIteratorTuple(@NotNull RocksDBIterator iterator,
																 @NotNull ReleasableSlice sliceMin,
																 @NotNull ReleasableSlice sliceMax,
																 @NotNull SafeCloseable seekTo) implements SafeCloseable {

	@Override
	public void close() {
		iterator.close();
		sliceMin.close();
		sliceMax.close();
		seekTo.close();
	}
}
