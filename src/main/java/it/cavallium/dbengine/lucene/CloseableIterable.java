package it.cavallium.dbengine.lucene;

import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.SafeCloseable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public interface CloseableIterable<T> extends Iterable<T>, DiscardingCloseable {

	@Override
	void close();

	@NotNull
	@Override
	Iterator<T> iterator();
}
