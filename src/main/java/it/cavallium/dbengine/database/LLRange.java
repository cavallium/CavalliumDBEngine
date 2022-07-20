package it.cavallium.dbengine.database;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.util.Send;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.StringJoiner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

/**
 * Range of data, from min (inclusive), to max (exclusive)
 */
public class LLRange extends SimpleResource {

	private static final LLRange RANGE_ALL = new LLRange((Buffer) null, (Buffer) null, (Buffer) null);
	@Nullable
	private final Buffer min;
	@Nullable
	private final Buffer max;
	@Nullable
	private final Buffer single;

	private LLRange(Send<Buffer> min, Send<Buffer> max, Send<Buffer> single) {
		super();
		assert single == null || (min == null && max == null);
		this.min = min != null ? min.receive().makeReadOnly() : null;
		this.max = max != null ? max.receive().makeReadOnly() : null;
		this.single = single != null ? single.receive().makeReadOnly() : null;
	}

	private LLRange(Buffer min, Buffer max, Buffer single) {
		super();
		assert single == null || (min == null && max == null);
		this.min = min != null ? min.makeReadOnly() : null;
		this.max = max != null ? max.makeReadOnly() : null;
		this.single = single != null ? single.makeReadOnly() : null;
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
		ensureOpen();
		return min == null && max == null && single == null;
	}

	public boolean isSingle() {
		ensureOpen();
		return single != null;
	}

	public boolean hasMin() {
		ensureOpen();
		return min != null || single != null;
	}

	public Send<Buffer> getMin() {
		ensureOpen();
		if (min != null) {
			// todo: use a read-only copy
			return min.copy().send();
		} else if (single != null) {
			// todo: use a read-only copy
			return single.copy().send();
		} else {
			return null;
		}
	}

	public Buffer getMinUnsafe() {
		ensureOpen();
		if (min != null) {
			return min;
		} else if (single != null) {
			return single;
		} else {
			return null;
		}
	}

	public Buffer getMinCopy() {
		ensureOpen();
		if (min != null) {
			return min.copy();
		} else if (single != null) {
			return single.copy();
		} else {
			return null;
		}
	}

	public boolean hasMax() {
		ensureOpen();
		return max != null || single != null;
	}

	public Send<Buffer> getMax() {
		ensureOpen();
		if (max != null) {
			// todo: use a read-only copy
			return max.copy().send();
		} else if (single != null) {
			// todo: use a read-only copy
			return single.copy().send();
		} else {
			return null;
		}
	}

	public Buffer getMaxUnsafe() {
		ensureOpen();
		if (max != null) {
			return max;
		} else if (single != null) {
			return single;
		} else {
			return null;
		}
	}

	public Buffer getMaxCopy() {
		ensureOpen();
		if (max != null) {
			return max.copy();
		} else if (single != null) {
			return single.copy();
		} else {
			return null;
		}
	}

	public Send<Buffer> getSingle() {
		ensureOpen();
		assert isSingle();
		// todo: use a read-only copy
		return single != null ? single.copy().send() : null;
	}

	public Buffer getSingleUnsafe() {
		ensureOpen();
		assert isSingle();
		return single;
	}

	@Override
	protected void ensureOpen() {
		super.ensureOpen();
		assert min == null || min.isAccessible() : "Range min not owned";
		assert max == null || max.isAccessible() : "Range max not owned";
		assert single == null || single.isAccessible() : "Range single not owned";
	}

	@Override
	protected void onClose() {
		if (min != null && min.isAccessible()) {
			min.close();
		}
		if (max != null && max.isAccessible()) {
			max.close();
		}
		if (single != null && single.isAccessible()) {
			single.close();
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
		ensureOpen();
		// todo: use a read-only copy
		return new LLRange(min != null ? min.copy().send() : null,
				max != null ? max.copy().send() : null,
				single != null ? single.copy().send(): null
		);
	}
}
