package it.cavallium.dbengine.utils;

import it.cavallium.dbengine.MetricUtils;
import it.cavallium.dbengine.database.SafeCloseable;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class SimpleResource implements SafeCloseable {

	protected static final boolean ENABLE_LEAK_DETECTION
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.leakdetection.enable", "true"));
	protected static final boolean ADVANCED_LEAK_DETECTION
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.leakdetection.advanced", "false"));
	private static final Logger LOG = LogManager.getLogger(SimpleResource.class);
	public static final Cleaner CLEANER = Cleaner.create();

	private final AtomicBoolean closed;
	private final boolean canClose;

	public SimpleResource() {
		this(true);
	}

	protected SimpleResource(boolean canClose) {
		this(canClose, new AtomicBoolean());
	}

	protected SimpleResource(AtomicBoolean closed) {
		this(true, closed);
	}

	private SimpleResource(boolean canClose, AtomicBoolean closed) {
		this.canClose = canClose;
		this.closed = closed;

		if (ENABLE_LEAK_DETECTION && canClose) {
			var resourceClass = this.getClass();
			Exception initializationStackTrace;
			if (ADVANCED_LEAK_DETECTION) {
				var stackTrace = Thread.currentThread().getStackTrace();
				initializationStackTrace = new Exception("Initialization point");
				initializationStackTrace.setStackTrace(stackTrace);
			} else {
				initializationStackTrace = null;
			}
			CLEANER.register(this, () -> {
				if (!closed.get()) {
					LOG.error("Resource leak of type {}", resourceClass, initializationStackTrace);
				}
			});
		}
	}

	@Override
	public final void close() {
		if (canClose && closed.compareAndSet(false, true)) {
			onClose();
		}
	}

	protected boolean isClosed() {
		return canClose && closed.get();
	}

	protected AtomicBoolean getClosed() {
		return closed;
	}

	protected void ensureOpen() {
		if (canClose && closed.get()) {
			throw new IllegalStateException("Resource is closed");
		}
	}

	protected abstract void onClose();
}
