package it.cavallium.dbengine.database;

import static io.netty5.buffer.Unpooled.wrappedBuffer;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.Send;
import io.netty5.buffer.api.internal.ResourceSupport;
import java.util.StringJoiner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

/**
 * Range of data, from min (inclusive),to max (exclusive)
 */
public class LLRange extends ResourceSupport<LLRange, LLRange> {

	private static final Logger logger = LogManager.getLogger(LLRange.class);

	private static final Drop<LLRange> DROP = new Drop<>() {
		@Override
		public void drop(LLRange obj) {
			try {
				if (obj.min != null) {
					obj.min.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close min", ex);
			}
			try {
				if (obj.max != null) {
					obj.max.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close max", ex);
			}
			try {
				if (obj.single != null) {
					obj.single.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close single", ex);
			}
		}

		@Override
		public Drop<LLRange> fork() {
			return this;
		}

		@Override
		public void attach(LLRange obj) {

		}
	};

	private static final LLRange RANGE_ALL = new LLRange((Buffer) null, (Buffer) null, (Buffer) null);
	@Nullable
	private Buffer min;
	@Nullable
	private Buffer max;
	@Nullable
	private Buffer single;

	private LLRange(Send<Buffer> min, Send<Buffer> max, Send<Buffer> single) {
		super(DROP);
		assert isAllAccessible();
		assert single == null || (min == null && max == null);
		this.min = min != null ? min.receive().makeReadOnly() : null;
		this.max = max != null ? max.receive().makeReadOnly() : null;
		this.single = single != null ? single.receive().makeReadOnly() : null;
	}

	private LLRange(Buffer min, Buffer max, Buffer single) {
		super(DROP);
		assert isAllAccessible();
		assert single == null || (min == null && max == null);
		this.min = min != null ? min.makeReadOnly() : null;
		this.max = max != null ? max.makeReadOnly() : null;
		this.single = single != null ? single.makeReadOnly() : null;
	}

	private boolean isAllAccessible() {
		assert min == null || min.isAccessible() : "Range min not owned";
		assert max == null || max.isAccessible() : "Range max not owned";
		assert single == null || single.isAccessible() : "Range single not owned";
		assert this.isAccessible() : "Range not accessible";
		assert this.isOwned() : "Range not owned";
		return true;
	}

	public static LLRange all() {
		return RANGE_ALL.copy();
	}

	public static LLRange from(Send<Buffer> min) {
		return new LLRange(min, null, null);
	}

	public static LLRange to(Send<Buffer> max) {
		return new LLRange(null, max, null);
	}

	public static LLRange single(Send<Buffer> single) {
		return new LLRange(null, null, single);
	}

	public static LLRange singleUnsafe(Buffer single) {
		return new LLRange(null, null, single);
	}

	public static LLRange of(Send<Buffer> min, Send<Buffer> max) {
		return new LLRange(min, max, null);
	}

	public static LLRange ofUnsafe(Buffer min, Buffer max) {
		return new LLRange(min, max, null);
	}

	public boolean isAll() {
		ensureOwned();
		return min == null && max == null && single == null;
	}

	public boolean isSingle() {
		ensureOwned();
		return single != null;
	}

	public boolean hasMin() {
		ensureOwned();
		return min != null || single != null;
	}

	public Send<Buffer> getMin() {
		ensureOwned();
		if (min != null) {
			return min.copy().send();
		} else if (single != null) {
			return single.copy().send();
		} else {
			return null;
		}
	}

	public Buffer getMinUnsafe() {
		ensureOwned();
		if (min != null) {
			return min;
		} else if (single != null) {
			return single;
		} else {
			return null;
		}
	}

	public boolean hasMax() {
		ensureOwned();
		return max != null || single != null;
	}

	public Send<Buffer> getMax() {
		ensureOwned();
		if (max != null) {
			return max.copy().send();
		} else if (single != null) {
			return single.copy().send();
		} else {
			return null;
		}
	}

	public Buffer getMaxUnsafe() {
		ensureOwned();
		if (max != null) {
			return max;
		} else if (single != null) {
			return single;
		} else {
			return null;
		}
	}

	public Send<Buffer> getSingle() {
		ensureOwned();
		assert isSingle();
		return single != null ? single.copy().send() : null;
	}

	public Buffer getSingleUnsafe() {
		ensureOwned();
		assert isSingle();
		return single;
	}

	private void ensureOwned() {
		assert isAllAccessible();
		if (!isOwned()) {
			if (!isAccessible()) {
				throw this.createResourceClosedException();
			} else {
				throw new IllegalStateException("Resource not owned");
			}
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLRange llRange = (LLRange) o;
		return LLUtils.equals(min, llRange.min) && LLUtils.equals(max, llRange.max);
	}

	@Override
	public int hashCode() {
		int result = LLUtils.hashCode(min);
		result = 31 * result + LLUtils.hashCode(max);
		return result;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLRange.class.getSimpleName() + "[", "]")
				.add("min=" + LLUtils.toString(min))
				.add("max=" + LLUtils.toString(max))
				.toString();
	}

	public LLRange copy() {
		ensureOwned();
		return new LLRange(min != null ? min.copy().send() : null,
				max != null ? max.copy().send() : null,
				single != null ? single.copy().send(): null
		);
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLRange> prepareSend() {
		Send<Buffer> minSend;
		Send<Buffer> maxSend;
		Send<Buffer> singleSend;
		minSend = this.min != null ? this.min.send() : null;
		maxSend = this.max != null ? this.max.send() : null;
		singleSend = this.single != null ? this.single.send() : null;
		return drop -> {
			var instance = new LLRange(minSend, maxSend, singleSend);
			drop.attach(instance);
			return instance;
		};
	}

	protected void makeInaccessible() {
		this.min = null;
		this.max = null;
		this.single = null;
	}
}
