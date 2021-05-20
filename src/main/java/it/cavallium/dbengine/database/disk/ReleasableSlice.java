package it.cavallium.dbengine.database.disk;

import io.netty.buffer.ByteBuf;
import org.rocksdb.AbstractSlice;

public interface ReleasableSlice {

	default void release() {

	}

	AbstractSlice<?> slice();

	ByteBuf byteBuf();

	Object additionalData();
}
