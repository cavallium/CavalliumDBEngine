package it.cavallium.dbengine.database;

import io.netty.buffer.ByteBuf;
import io.netty.util.IllegalReferenceCountException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;

public class LLEntry {

	private static final Logger logger = LoggerFactory.getLogger(LLEntry.class);

	private final AtomicInteger refCnt = new AtomicInteger(1);

	private final ByteBuf key;
	private final ByteBuf value;

	public LLEntry(ByteBuf key, ByteBuf value) {
		try {
			this.key = key.retain();
			this.value = value.retain();
		} finally {
			key.release();
			value.release();
		}
	}

	public ByteBuf getKey() {
		if (refCnt.get() <= 0) {
			throw new IllegalReferenceCountException(refCnt.get());
		}
		return key;
	}

	public ByteBuf getValue() {
		if (refCnt.get() <= 0) {
			throw new IllegalReferenceCountException(refCnt.get());
		}
		return value;
	}

	public void retain() {
		if (refCnt.getAndIncrement() <= 0) {
			throw new IllegalReferenceCountException(refCnt.get(), 1);
		}
		key.retain();
		value.retain();
	}

	public void release() {
		if (refCnt.decrementAndGet() < 0) {
			throw new IllegalReferenceCountException(refCnt.get(), -1);
		}
		if (key.refCnt() > 0) {
			key.release();
		}
		if (value.refCnt() > 0) {
			value.release();
		}
	}

	public boolean isReleased() {
		return refCnt.get() <= 0;
	}

	@Override
	protected void finalize() throws Throwable {
		if (refCnt.get() > 0) {
			logger.warn(this.getClass().getName() + "::release has not been called!");
		}
		super.finalize();
	}
}
