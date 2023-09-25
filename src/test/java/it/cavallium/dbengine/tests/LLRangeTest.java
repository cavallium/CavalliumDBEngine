package it.cavallium.dbengine.tests;

import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLLocalDictionary;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;

class LLRangeTest {

	private void printRanges(LLRange range, List<LLRange> ranges) {
		for (int i = 1, size = ranges.size(); i < size; i++) {
			LLRange prev = ranges.get(i - 1);
			LLRange cur = ranges.get(i);
			assertEquals(0, Objects.compare(prev.getMax(), cur.getMin(), Comparator.nullsFirst(Comparator.naturalOrder())));
		}
		System.out.println(toStringRange(range) + " ---> " + String.join(", ", ranges.stream().map(this::toStringRange).toList()));
	}

	private String toStringRange(LLRange r) {
		if (r.isSingle()) {
			return LLUtils.HEX_FORMAT.formatHex(r.getSingle().toByteArray());
		} else if (r.hasMin() && r.hasMax()) {
			return LLUtils.HEX_FORMAT.formatHex(r.getMin().toByteArray())
					+ "-"
					+ LLUtils.HEX_FORMAT.formatHex(r.getMax().toByteArray());
		} else if (r.hasMin()) {
			return LLUtils.HEX_FORMAT.formatHex(r.getMin().toByteArray()) + "-MAX";
		} else if (r.hasMax()) {
			return "MIN-" + LLUtils.HEX_FORMAT.formatHex(r.getMax().toByteArray());
		} else {
			return "MIN-MAX";
		}
	}

	@Test
	void parallelizeRange() {
		var minBi = BigInteger.valueOf(50);
		var maxBi = BigInteger.valueOf(100);
		var range = LLRange.of(Buf.wrap(minBi.toByteArray()), Buf.wrap(maxBi.toByteArray()));
		var ranges = LLLocalDictionary.parallelizeRange(range, 8).toList();
		printRanges(range, ranges);
	}

	@Test
	void parallelizeRangeNegative() {
		var minBi = BigInteger.valueOf(Byte.MIN_VALUE);
		var maxBi = BigInteger.valueOf(-50);
		var range = LLRange.of(Buf.wrap(minBi.toByteArray()), Buf.wrap(maxBi.toByteArray()));
		var ranges = LLLocalDictionary.parallelizeRange(range, 8).toList();
		printRanges(range, ranges);
	}

	@Test
	void parallelizeRangeNegativeToPositive() {
		var minBi = BigInteger.valueOf(0);
		var maxBi = BigInteger.valueOf(Byte.MIN_VALUE);
		var range = LLRange.of(Buf.wrap(minBi.toByteArray()), Buf.wrap(maxBi.toByteArray()));
		var ranges = LLLocalDictionary.parallelizeRange(range, 8).toList();
		printRanges(range, ranges);
	}

	@Test
	void parallelizeRangeMin() {
		var minBi = BigInteger.valueOf(50);
		var range = LLRange.of(Buf.wrap(minBi.toByteArray()), null);
		var ranges = LLLocalDictionary.parallelizeRange(range, 8).toList();
		printRanges(range, ranges);
	}

	@Test
	void parallelizeRangeMax() {
		var maxBi = BigInteger.valueOf(50);
		var range = LLRange.of(null, Buf.wrap(maxBi.toByteArray()));
		var ranges = LLLocalDictionary.parallelizeRange(range, 8).toList();
		printRanges(range, ranges);
	}

	@ParameterizedTest
	@MethodSource("provideRanges")
	public void intersectTest(IntersectArgs args) {
		Assertions.assertEquals(args.expected, LLRange.intersect(args.rangeA, args.rangeB));
	}

	public record IntersectArgs(LLRange expected, LLRange rangeA, LLRange rangeB) {}

	public static Stream<IntersectArgs> provideRanges() {
		return Stream.of(
				new IntersectArgs(LLRange.all(), LLRange.all(), LLRange.all()),
				new IntersectArgs(LLRange.single(Buf.wrap((byte) 1)), LLRange.single(Buf.wrap((byte) 1)), LLRange.all()),
				new IntersectArgs(LLRange.single(Buf.wrap((byte) 1)), LLRange.all(), LLRange.single(Buf.wrap((byte) 1))),
				new IntersectArgs(null, LLRange.single(Buf.wrap((byte) 1)), LLRange.single(Buf.wrap((byte) 2))),
				new IntersectArgs(null, LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2)), LLRange.of(Buf.wrap((byte) 2), Buf.wrap((byte) 3))),
				new IntersectArgs(LLRange.single(Buf.wrap((byte) 1)), LLRange.single(Buf.wrap((byte) 1)), LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2))),
				new IntersectArgs(LLRange.single(Buf.wrap((byte) 1)), LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2)), LLRange.single(Buf.wrap((byte) 1))),
				new IntersectArgs(null, LLRange.single(Buf.wrap((byte) 2)), LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2))),
				new IntersectArgs(null, LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2)), LLRange.single(Buf.wrap((byte) 2))),
				new IntersectArgs(null, LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2)), LLRange.of(Buf.wrap((byte) 3), Buf.wrap((byte) 4))),
				new IntersectArgs(null, LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2)), LLRange.of(Buf.wrap((byte) 2), Buf.wrap((byte) 3))),
				new IntersectArgs(LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2)), LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2)), LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 3))),
				new IntersectArgs(LLRange.of(Buf.wrap((byte) 4), Buf.wrap((byte) 7)), LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 7)), LLRange.of(Buf.wrap((byte) 4), Buf.wrap((byte) 9))),
				new IntersectArgs(LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2)), LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2)), LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2))),
				new IntersectArgs(null, LLRange.of(Buf.wrap((byte) 1), Buf.wrap((byte) 2)), LLRange.of(Buf.wrap((byte) 3), Buf.wrap((byte) 4)))
		);
	}
}