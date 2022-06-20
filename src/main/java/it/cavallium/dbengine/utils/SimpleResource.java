package it.cavallium.dbengine.utils;

import it.cavallium.dbengine.database.SafeCloseable;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SimpleResource implements SafeCloseable {

	private final AtomicBoolean closed = new AtomicBoolean();
	private final boolean canClose;

	public SimpleResource() {
		canClose = true;
	}

	protected SimpleResource(boolean canClose) {
		this.canClose = canClose;
	}

	@Override
	public final void close() {
		if (canClose && closed.compareAndSet(false, true)) {
			onClose();
		}
	}

	private boolean isClosed() {
		return canClose && closed.get();
	}

	protected void ensureOpen() {
		if (canClose && closed.get()) {
			throw new IllegalStateException("Resource is closed");
		}
	}

	protected abstract void onClose();
}
