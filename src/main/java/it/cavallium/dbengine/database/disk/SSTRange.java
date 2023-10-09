package it.cavallium.dbengine.database.disk;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import java.util.HexFormat;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public sealed interface SSTRange {

	boolean isBounded();

	sealed interface SSTLLRange extends SSTRange permits SSTRangeNone, SSTRangeFull, SSTRangeKey, SSTSingleKey {

		@Nullable LLRange toLLRange();

		default boolean isBounded() {
			LLRange r = toLLRange();
			return r == null || LLUtils.isBoundedRange(r);
		}
	}

	;

	static SSTRange parse(String raw) {
		var parts = StringUtils.split(raw, '-');
		return switch (parts[0]) {
			case "none" -> new SSTRangeNone();
			case "full" -> new SSTRangeFull();
			case "offset" -> new SSTRangeOffset(parts[1].isBlank() ? null : Long.parseUnsignedLong(parts[1]),
					parts[2].isBlank() ? null : Long.parseUnsignedLong(parts[2])
			);
			case "key" -> new SSTRangeKey(parts[1].isBlank() ? null : Buf.wrap(LLUtils.parseHex(parts[1])),
					parts[2].isBlank() ? null : Buf.wrap(LLUtils.parseHex(parts[2]))
			);
			case "single-key" -> new SSTSingleKey(Buf.wrap(LLUtils.parseHex(parts[1])));
			default -> throw new IllegalStateException("Unexpected value: " + parts[0]);
		};
	}

	static SSTRange parse(LLRange range) {
		if (range == null) {
			return new SSTRangeNone();
		}
		return range.isAll() ? new SSTRangeFull()
				: range.isSingle() ? new SSTSingleKey(range.getSingle()) : new SSTRangeKey(range.getMin(), range.getMax());
	}

	record SSTRangeNone() implements SSTRange, SSTLLRange {

		@Override
		public String toString() {
			return "none";
		}

		public LLRange toLLRange() {
			return null;
		}
	}

	record SSTRangeFull() implements SSTRange, SSTLLRange {

		@Override
		public String toString() {
			return "full";
		}

		public LLRange toLLRange() {
			return LLRange.all();
		}
	}

	record SSTRangeOffset(@Nullable Long offsetMin, @Nullable Long offsetMax) implements SSTRange {

		@Override
		public String toString() {
			return "offset-" + (offsetMin != null ? offsetMin : "") + "-" + (offsetMax != null ? offsetMax : "");
		}

		@Override
		public boolean isBounded() {
			return offsetMin != null && offsetMax != null;
		}
	}

	record SSTRangeKey(@Nullable Buf min, @Nullable Buf max) implements SSTRange, SSTLLRange {

		private static final HexFormat HF = HexFormat.of();

		@Override
		public String toString() {
			return "key-" + (min != null ? HF.formatHex(min.asUnboundedArray(), 0, min.size()) : "") + "-" + (max != null
					? HF.formatHex(max.asUnboundedArray(), 0, max.size()) : "");
		}

		public LLRange toLLRange() {
			return LLRange.of(min, max);
		}
	}

	record SSTSingleKey(@NotNull Buf key) implements SSTRange, SSTLLRange {

		private static final HexFormat HF = HexFormat.of();

		@Override
		public String toString() {
			return "single-key-" + HF.formatHex(key.asUnboundedArray(), 0, key.size());
		}

		public LLRange toLLRange() {
			return LLRange.single(key);
		}
	}
}
