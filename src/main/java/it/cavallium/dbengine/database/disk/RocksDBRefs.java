package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.SafeCloseable;
import java.util.ArrayList;
import org.rocksdb.AbstractImmutableNativeReference;

public final class RocksDBRefs implements DiscardingCloseable {

	private final ArrayList<AbstractImmutableNativeReference> list = new ArrayList<>();
	private boolean closed;

	public RocksDBRefs() {
	}

	public RocksDBRefs(Iterable<? extends AbstractImmutableNativeReference> it) {
		it.forEach(list::add);
	}

	public synchronized void track(AbstractImmutableNativeReference ref) {
		if (closed) {
			throw new IllegalStateException("Closed");
		}
		list.add(ref);
	}

	@Override
	public synchronized void close() {
		if (!closed) {
			closed = true;
			for (int i = list.size() - 1; i >= 0; i--) {
				var ref = list.get(i);

				if (ref.isOwningHandle()) {
					ref.close();
				}
			}
			list.clear();
			list.trimToSize();
		}
	}
}
