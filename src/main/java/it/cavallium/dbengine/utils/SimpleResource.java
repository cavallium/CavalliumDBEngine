package it.cavallium.dbengine.utils;

import it.cavallium.dbengine.database.SafeCloseable;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SimpleResource implements SafeCloseable {

	private final AtomicBoolean closed = new AtomicBoolean();

	@Override
	public final void close() {
		if (closed.compareAndSet(false, true)) {
			onClose();
		}
	}

	private boolean isClosed() {
		return closed.get();
	}

	protected void ensureOpen() {
		if (closed.get()) {
			throw new IllegalStateException("Resource is closed");
		}
	}

	protected abstract void onClose();
}
