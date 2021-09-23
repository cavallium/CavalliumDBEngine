package it.cavallium.dbengine.database;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.internal.LifecycleTracer;
import io.net5.buffer.api.internal.ResourceSupport;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;

public abstract class LiveResourceSupport<I extends Resource<I>, T extends LiveResourceSupport<I, T>> extends ResourceSupport<I, T> {

	private static final Logger logger = LoggerFactory.getLogger(LiveResourceSupport.class);

	protected LiveResourceSupport(Drop<T> drop) {
		super(drop);
	}

	@Override
	protected void finalize() throws Throwable {
		if (this.isAccessible()) {
			try {
				this.close();
			} catch (Throwable ignored) {
			} finally {
				var ise = new IllegalStateException("Resource not released");
				ise.setStackTrace(new StackTraceElement[0]);
				logger.error("Resource not released: {}", this, attachTrace(ise));
			}
		}
		super.finalize();
	}
}
