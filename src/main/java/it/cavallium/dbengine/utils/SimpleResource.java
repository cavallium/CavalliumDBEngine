package it.cavallium.dbengine.utils;

import it.cavallium.dbengine.MetricUtils;
import it.cavallium.dbengine.database.SafeCloseable;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

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

	public SimpleResource(@Nullable Runnable cleanerAction) {
		this(true, cleanerAction);
	}

	protected SimpleResource(boolean canClose) {
		this(canClose, null);
	}

	private SimpleResource(boolean canClose, @Nullable Runnable cleanerAction) {
		this.canClose = canClose;
		var closed = new AtomicBoolean();
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
					if (cleanerAction != null) {
						cleanerAction.run();
					}
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

	public boolean isClosed() {
		return canClose && closed.get();
	}

	protected AtomicBoolean getClosed() {
		return closed;
	}

	protected void ensureOpen() {
		if (isClosed()) {
			throw new IllegalStateException("Resource is closed");
		}
	}

	protected abstract void onClose();
}
