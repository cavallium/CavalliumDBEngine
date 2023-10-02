package it.cavallium.dbengine.repair;

import java.io.Serial;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public final class DataSize extends Number implements Comparable<DataSize> {

	@Serial
	private static final long serialVersionUID = 7213411239846723568L;

	public static DataSize ZERO = new DataSize(0L);
	public static DataSize ONE = new DataSize(1L);
	public static DataSize KIB = new DataSize(1024L);
	public static DataSize KB = new DataSize(1000L);
	public static DataSize MIB = new DataSize(1024L * 1024);
	public static DataSize MB = new DataSize(1000L * 1000);
	public static DataSize GIB = new DataSize(1024L * 1024 * 1024);
	public static DataSize GB = new DataSize(1000L * 1000 * 1000);
	public static DataSize TIB = new DataSize(1024L * 1024 * 1024 * 1024);
	public static DataSize TB = new DataSize(1000L * 1000 * 1000 * 1024);
	public static DataSize PIB = new DataSize(1024L * 1024 * 1024 * 1024 * 1024);
	public static DataSize PB = new DataSize(1000L * 1000 * 1000 * 1024 * 1024);
	public static DataSize EIB = new DataSize(1024L * 1024 * 1024 * 1024 * 1024 * 1024);
	public static DataSize EB = new DataSize(1000L * 1000 * 1000 * 1024 * 1024 * 1024);
	public static DataSize MAX_VALUE = new DataSize(Long.MAX_VALUE);

	private final long size;

	public DataSize(long size) {
		this.size = size;
	}

	public DataSize(String size) {
		size = size.replaceAll("\\s|_", "");
		switch (size) {
			case "", "0", "-0", "+0" -> {
				this.size = 0;
				return;
			}
			case "∞", "inf", "infinite", "∞b" -> {
				this.size = Long.MAX_VALUE;
				return;
			}
		}
		int numberStartOffset = 0;
		int numberEndOffset = 0;
		boolean negative = false;
		{
			boolean firstChar = true;
			boolean numberMode = true;
			for (char c : size.toCharArray()) {
				if (c == '-') {
					if (firstChar) {
						negative = true;
						numberStartOffset++;
						numberEndOffset++;
					} else {
						throw new IllegalArgumentException("Found a minus character after index 0");
					}
				} else if (Character.isDigit(c)) {
					if (numberMode) {
						numberEndOffset++;
					} else {
						throw new IllegalArgumentException("Found a number after the unit");
					}
				} else if (Character.isLetter(c)) {
					if (numberEndOffset - numberStartOffset <= 0) {
						throw new IllegalArgumentException("No number found");
					}
					if (numberMode) {
						numberMode = false;
					}
				} else {
					throw new IllegalArgumentException("Unsupported character");
				}
				if (firstChar) {
					firstChar = false;
				}
			}
		}
		var number = Long.parseUnsignedLong(size, numberStartOffset, numberEndOffset, 10);
		if (numberEndOffset == size.length()) {
			// No measurement
			this.size = (negative ? -1 : 1) * number;
			return;
		}
		// Measurements are like B, MB, or MiB, not longer
		if (size.length() - numberEndOffset > 3) {
			throw new IllegalArgumentException("Wrong measurement unit");
		}
		var scaleChar = size.charAt(numberEndOffset);
		boolean powerOf2 = numberEndOffset + 1 < size.length() && size.charAt(numberEndOffset + 1) == 'i';
		int k = powerOf2 ? 1024 : 1000;
		var scale = switch (scaleChar) {
			case 'B' -> 1;
			case 'b' -> throw new IllegalArgumentException("Bits are not allowed");
			case 'K', 'k' -> k;
			case 'M', 'm' -> k * k;
			case 'G', 'g' -> k * k * k;
			case 'T', 't' -> k * k * k * k;
			case 'P', 'p' -> k * k * k * k * k;
			case 'E', 'e' -> k * k * k * k * k * k;
			case 'Z', 'z' -> k * k * k * k * k * k * k;
			case 'Y', 'y' -> k * k * k * k * k * k * k * k;
			default -> throw new IllegalStateException("Unexpected value: " + scaleChar);
		};
		// if scale is 1, the unit should be "B", nothing more
		if (scale == 1 && numberEndOffset + 1 != size.length()) {
			throw new IllegalArgumentException("Invalid unit");
		}
		this.size = (negative ? -1 : 1) * number * scale;
	}

	public static Long get(DataSize value) {
		if (value == null) {
			return null;
		} else {
			return value.size;
		}
	}

	public static long getOrElse(DataSize value, @NotNull DataSize defaultValue) {
		if (value == null) {
			return defaultValue.size;
		} else {
			return value.size;
		}
	}


	@Override
	public int intValue() {
		if (size >= Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		return (int) size;
	}

	@Override
	public long longValue() {
		return size;
	}

	@Override
	public float floatValue() {
		return size;
	}

	@Override
	public double doubleValue() {
		return size;
	}

	@Override
	public String toString() {
		return toString(true);
	}

	public String toString(boolean precise) {
		boolean siUnits = size % 1000 == 0;
		int k = siUnits ? 1000 : 1024;
		long lSize = size;
		CharacterIterator ci = new StringCharacterIterator((siUnits ? "k" : "K") + "MGTPEZY");
		while ((precise ? lSize % k == 0 : lSize > k) && lSize != 0) {
			lSize /= k;
			ci.next();
		}
		if (lSize == size) {
			return lSize + "B";
		}
		return lSize + "" + ci.previous() + (siUnits ? "B" : "iB");
	}

	@Override
	public int compareTo(@NotNull DataSize anotherLong) {
		return Long.compare(this.size, anotherLong.size);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DataSize) {
			return size == ((DataSize)obj).size;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(size);
	}
}
