package it.cavallium.dbengine.lucene;

import io.netty5.buffer.api.Buffer;
import java.util.function.Function;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.NumericUtils;
import org.rocksdb.AbstractComparator;

public interface HugePqCodec<T> {

	Buffer serialize(Function<Integer, Buffer> allocator, T data);

	T deserialize(Buffer b);

	default T clone(T obj) {
		return obj;
	}

	default AbstractComparator getComparator() {
		return null;
	}

	static int getLexInt(Buffer buffer, int offset, boolean invert) {
		var data = new byte[Integer.BYTES];
		buffer.copyInto(offset, data, 0, data.length);
		var result = sortableBytesToInt(data, 0, invert);
		return result;
	}

	static Buffer setLexInt(Buffer buffer, int offset, boolean invert, int value) {
		var data = new byte[Integer.BYTES];
		intToSortableBytes(value, data, 0, invert);
		for (int i = 0; i < data.length; i++) {
			buffer.setByte(offset + i, data[i]);
		}
		return buffer;
	}

	static float getLexFloat(Buffer buffer, int offset, boolean invert) {
		return sortableIntToFloat(getLexInt(buffer, offset, false), invert);
	}

	static Buffer setLexFloat(Buffer buffer, int offset, boolean invert, float value) {
		return setLexInt(buffer, offset, false, floatToSortableInt(value, invert));
	}

	/**
	 * Encodes an integer {@code value} such that unsigned byte order comparison is consistent with
	 * {@link Integer#compare(int, int)}
	 *
	 */
	public static void intToSortableBytes(int value, byte[] result, int offset, boolean invert) {
		if (!invert) {
			// Flip the sign bit, so negative ints sort before positive ints correctly:
			value ^= 0x80000000;
		} else {
			value ^= 0x7FFFFFFF;
		}
		BitUtil.VH_BE_INT.set(result, offset, value);
	}

	/**
	 * Decodes an integer value previously written with {@link #intToSortableBytes}
	 *
	 */
	public static int sortableBytesToInt(byte[] encoded, int offset, boolean invert) {
		int x = (int) BitUtil.VH_BE_INT.get(encoded, offset);
		if (!invert) {
			// Re-flip the sign bit to restore the original value:
			return x ^ 0x80000000;
		} else {
			return x ^ 0x7FFFFFFF;
		}
	}

	/**
	 * Converts a <code>float</code> value to a sortable signed <code>int</code>. The value is
	 * converted by getting their IEEE 754 floating-point &quot;float format&quot; bit layout and then
	 * some bits are swapped, to be able to compare the result as int. By this the precision is not
	 * reduced, but the value can easily used as an int. The sort order (including {@link Float#NaN})
	 * is defined by {@link Float#compareTo}; {@code NaN} is greater than positive infinity.
	 *
	 * @see #sortableIntToFloat
	 */
	public static int floatToSortableInt(float value, boolean invert) {
		if (invert) {
			return Float.floatToIntBits(value);
		} else {
			return NumericUtils.floatToSortableInt(value);
		}
	}

	/**
	 * Converts a sortable <code>int</code> back to a <code>float</code>.
	 *
	 * @see #floatToSortableInt
	 */
	public static float sortableIntToFloat(int encoded, boolean invert) {
		if (invert) {
			return Float.intBitsToFloat(encoded);
		} else {
			return NumericUtils.sortableIntToFloat(encoded);
		}
	}
}
