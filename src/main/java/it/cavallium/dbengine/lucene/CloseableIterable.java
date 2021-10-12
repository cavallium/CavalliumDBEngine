package it.cavallium.dbengine.lucene;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public interface CloseableIterable<T> extends Iterable<T>, Closeable {

	@Override
	void close();

	@NotNull
	@Override
	Iterator<T> iterator();
}
