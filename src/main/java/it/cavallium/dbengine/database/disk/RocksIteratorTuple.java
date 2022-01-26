package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksIterator;

public record RocksIteratorTuple(@NotNull RocksIterator iterator, @NotNull ReleasableSlice sliceMin,
																 @NotNull ReleasableSlice sliceMax, @NotNull SafeCloseable seekTo) implements
		SafeCloseable {

	@Override
	public void close() {
		iterator.close();
		sliceMin.close();
		sliceMax.close();
		seekTo.close();
	}
}
