package it.cavallium.dbengine.utils;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.Random;
import org.jetbrains.annotations.NotNull;

/**
 * Linear feedback shift register
 * <p>
 * Taps can be found at: See http://www.xilinx.com/support/documentation/application_notes/xapp052.pdf See
 * http://mathoverflow.net/questions/46961/how-are-taps-proven-to-work-for-lfsrs/46983#46983 See
 * http://www.newwaveinstruments.com/resources/articles/m_sequence_linear_feedback_shift_register_lfsr.htm See
 * http://www.yikes.com/~ptolemy/lfsr_web/index.htm See http://seanerikoconnor.freeservers.com/Mathematics/AbstractAlgebra/PrimitivePolynomials/overview.html
 *
 * @author OldCurmudgeon
 */
public class LFSR implements Iterable<BigInteger> {

	private static final Random random = new Random();

	// Bit pattern for taps.
	private final BigInteger taps;
	// Where to start (and end).
	private final BigInteger start;

	public static LFSR randomInt() {
		return random(32, random.nextInt());
	}

	public static LFSR randomLong() {
		return random(64, random.nextLong());
	}

	public static LFSR randomPositiveLong() {
		return random(50, Math.abs(random.nextInt()));
	}

	public static BigInteger randomPrimitive(int bitsSize) {
		// Build the BigInteger.
		BigInteger primitive = BigInteger.ZERO;
		for (int bitNumber = 0; bitNumber <= bitsSize; bitNumber++) {
			if (random.nextBoolean() || bitNumber == 0 || bitNumber == bitsSize) {
				primitive = primitive.or(BigInteger.ONE.shiftLeft(bitNumber));
			}
		}
		return primitive;
	}

	public static LFSR random(int bitsSize, long startNumber) {
		return new LFSR(randomPrimitive(bitsSize), BigInteger.valueOf(startNumber));
	}

	// The poly must be primitive to span the full sequence.
	public LFSR(BigInteger primitivePoly, BigInteger start) {
		// Where to start from (and stop).
		this.start = start.equals(BigInteger.ZERO) ? BigInteger.ONE : start;
		// Knock off the 2^0 coefficient of the polynomial for the TAP.
		this.taps = primitivePoly.shiftRight(1);
	}

	@NotNull
	@Override
	public LFSRIterator iterator() {
		return new LFSRIterator(start);
	}

	public class LFSRIterator implements Iterator<BigInteger> {
		// The last one we returned.

		private BigInteger last = null;
		// The next one to return.
		private BigInteger next = null;

		public LFSRIterator(BigInteger start) {
			// Do not return the seed.
			last = start;
		}

		@Override
		public boolean hasNext() {
			if (next == null) {
				/*
				 * Uses the Galois form.
				 *
				 * Shift last right one.
				 *
				 * If the bit shifted out was a 1 - xor with the tap mask.
				 */
				boolean shiftedOutA1 = last.testBit(0);
				// Shift right.
				next = last.shiftRight(1);
				if (shiftedOutA1) {
					// Tap!
					next = next.xor(taps);
				}
				// Never give them `start` again.
				if (next.equals(start)) {
					// Could set a finished flag here too.
					next = null;
				}
			}
			return next != null;
		}

		@Override
		public BigInteger next() {
			// Remember this one.
			last = hasNext() ? next : null;
			// Don't deliver it again.
			next = null;
			return last;
		}

		public BigInteger next(BigInteger last) {
			this.last = last;
			next = null;
			return next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Not supported.");
		}

		@Override
		public String toString() {
			return LFSR.this.toString() + "[" + (last != null ? last.toString(16) : "") + "-" + (next != null ? next
					.toString(16) : "") + "]";
		}
	}

	@Override
	public String toString() {
		return "(" + taps.toString(32) + ")-" + start.toString(32);
	}
}