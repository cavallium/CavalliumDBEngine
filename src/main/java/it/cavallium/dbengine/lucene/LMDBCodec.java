package it.cavallium.dbengine.lucene;

import io.net5.buffer.ByteBuf;
import java.util.function.Function;

public interface LMDBCodec<T> {

	ByteBuf serialize(Function<Integer, ByteBuf> allocator, T data);

	T deserialize(ByteBuf b);
}
