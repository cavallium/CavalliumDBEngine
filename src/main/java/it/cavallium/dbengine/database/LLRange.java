package it.cavallium.dbengine.database;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.buffer.Unpooled.wrappedUnmodifiableBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Range of data, from min (inclusive),to max (exclusive)
 */
public class LLRange {

	private static final LLRange RANGE_ALL = new LLRange(null, null);
	private final ByteBuf min;
	private final ByteBuf max;
	private final AtomicInteger refCnt = new AtomicInteger(1);

	private LLRange(ByteBuf min, ByteBuf max) {
		assert min == null || min.refCnt() > 0;
		assert max == null || max.refCnt() > 0;
		this.min = min;
		this.max = max;
	}

	public static LLRange all() {
		return RANGE_ALL;
	}

	public static LLRange from(ByteBuf min) {
		return new LLRange(min, null);
	}

	public static LLRange to(ByteBuf max) {
		return new LLRange(null, max);
	}

	public static LLRange single(ByteBuf single) {
		try {
			return new LLRange(single.retain(), single.retain());
		} finally {
			single.release();
		}
	}

	public static LLRange of(ByteBuf min, ByteBuf max) {
		return new LLRange(min, max);
	}

	public boolean isAll() {
		checkReleased();
		assert min == null || min.refCnt() > 0;
		assert max == null || max.refCnt() > 0;
		return min == null && max == null;
	}

	public boolean isSingle() {
		checkReleased();
		assert min == null || min.refCnt() > 0;
		assert max == null || max.refCnt() > 0;
		if (min == null || max == null) return false;
		return LLUtils.equals(min, max);
	}

	public boolean hasMin() {
		checkReleased();
		assert min == null || min.refCnt() > 0;
		assert max == null || max.refCnt() > 0;
		return min != null;
	}

	public ByteBuf getMin() {
		checkReleased();
		assert min == null || min.refCnt() > 0;
		assert max == null || max.refCnt() > 0;
		assert min != null;
		return min;
	}

	public boolean hasMax() {
		checkReleased();
		assert min == null || min.refCnt() > 0;
		assert max == null || max.refCnt() > 0;
		return max != null;
	}

	public ByteBuf getMax() {
		checkReleased();
		assert min == null || min.refCnt() > 0;
		assert max == null || max.refCnt() > 0;
		assert max != null;
		return max;
	}

	public ByteBuf getSingle() {
		checkReleased();
		assert min == null || min.refCnt() > 0;
		assert max == null || max.refCnt() > 0;
		assert isSingle();
		return min;
	}

	private void checkReleased() {
		if (refCnt.get() <= 0) {
			throw new IllegalStateException("Released");
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

	public LLRange retain() {
		if (refCnt.updateAndGet(refCnt -> refCnt <= 0 ? 0 : (refCnt + 1)) <= 0) {
			throw new IllegalStateException("Released");
		}
		if (min != null) {
			min.retain();
		}
		if (max != null) {
			max.retain();
		}
		return this;
	}

	public void release() {
		if (refCnt.decrementAndGet() < 0) {
			throw new IllegalStateException("Already released");
		}
		if (min != null) {
			min.release();
		}
		if (max != null) {
			max.release();
		}
	}
}
