package it.cavallium.dbengine.database;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Drop;
import io.netty.buffer.api.Owned;
import io.netty.buffer.api.Send;
import io.netty.buffer.api.internal.ResourceSupport;
import java.util.StringJoiner;

/**
 * Range of data, from min (inclusive),to max (exclusive)
 */
public class LLRange extends ResourceSupport<LLRange, LLRange> {

	private static final LLRange RANGE_ALL = new LLRange(null, null, null, d -> {});
	private Buffer min;
	private Buffer max;
	private Buffer single;

	private LLRange(Send<Buffer> min, Send<Buffer> max, Send<Buffer> single, Drop<LLRange> drop) {
		super(new CloseOnDrop(drop));
		assert isAllAccessible();
		assert single == null || (min == null && max == null);
		this.min = min != null ? min.receive().makeReadOnly() : null;
		this.max = max != null ? max.receive().makeReadOnly() : null;
		this.single = single != null ? single.receive().makeReadOnly() : null;
	}

	private boolean isAllAccessible() {
		assert min == null || min.isAccessible();
		assert max == null || max.isAccessible();
		assert single == null || single.isAccessible();
		assert this.isAccessible();
		assert this.isOwned();
		return true;
	}

	public static LLRange all() {
		return RANGE_ALL.copy();
	}

	public static LLRange from(Send<Buffer> min) {
		return new LLRange(min, null, null, d -> {});
	}

	public static LLRange to(Send<Buffer> max) {
		return new LLRange(null, max, null, d -> {});
	}

	public static LLRange single(Send<Buffer> single) {
		return new LLRange(null, null, single, d -> {});
	}

	public static LLRange of(Send<Buffer> min, Send<Buffer> max) {
		return new LLRange(min, max, null, d -> {});
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
				single != null ? single.copy().send(): null,
				d -> {}
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
		this.makeInaccessible();
		return drop -> new LLRange(minSend, maxSend, singleSend, drop);
	}

	private void makeInaccessible() {
		this.min = null;
		this.max = null;
		this.single = null;
	}

	private static class CloseOnDrop implements Drop<LLRange> {

		private final Drop<LLRange> delegate;

		public CloseOnDrop(Drop<LLRange> drop) {
			this.delegate = drop;
		}

		@Override
		public void drop(LLRange obj) {
			if (obj.min != null) obj.min.close();
			if (obj.max != null) obj.max.close();
			if (obj.single != null) obj.single.close();
			obj.makeInaccessible();
			delegate.drop(obj);
		}
	}
}
