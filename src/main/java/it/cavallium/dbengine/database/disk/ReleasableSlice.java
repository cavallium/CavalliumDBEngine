package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Buffer;
import it.cavallium.dbengine.database.SafeCloseable;
import org.rocksdb.AbstractSlice;

public interface ReleasableSlice extends SafeCloseable {

	@Override
	default void close() {

	}

	AbstractSlice<?> slice();

	Buffer byteBuf();

	Object additionalData();
}
