package it.cavallium.dbengine.lucene;

import io.netty5.buffer.ByteBuf;
import java.util.Comparator;
import java.util.function.Function;

public interface LMDBSortedCodec<T> extends LMDBCodec<T> {

	int compare(T o1, T o2);

	int compareDirect(ByteBuf o1, ByteBuf o2);

}
