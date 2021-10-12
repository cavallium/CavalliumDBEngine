package it.cavallium.dbengine.lucene;

import io.net5.buffer.ByteBuf;
import java.util.Comparator;
import java.util.function.Function;

public interface LMDBCodec<T> {

	ByteBuf serialize(Function<Integer, ByteBuf> allocator, T data);

	T deserialize(ByteBuf b);

	int compare(T o1, T o2);

	int compareDirect(ByteBuf o1, ByteBuf o2);

}
