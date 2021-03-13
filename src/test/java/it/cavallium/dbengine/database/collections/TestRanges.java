package it.cavallium.dbengine.database.collections;

import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRanges {
	@Test
	public void testNextRangeKey() {
		testNextRangeKey(new byte[] {0x00, 0x00, 0x00});
		testNextRangeKey(new byte[] {0x00, 0x00, 0x01});
		testNextRangeKey(new byte[] {0x00, 0x00, 0x02});
		testNextRangeKey(new byte[] {0x00, 0x01, 0x02});
		testNextRangeKey(new byte[] {0x00, 0x00, (byte) 0xFF});
		testNextRangeKey(new byte[] {0x00, 0x01, (byte) 0xFF});
		testNextRangeKey(new byte[] {0x00, (byte) 0xFF, (byte) 0xFF});
		testNextRangeKey(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
		testNextRangeKey(new byte[] {(byte) 0xFF, (byte) 0, (byte) 0xFF});
		testNextRangeKey(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0});
	}

	public void testNextRangeKey(byte[] prefixKey) {

		byte[] firstRangeKey = DatabaseMapDictionaryDeep.firstRangeKey(prefixKey, prefixKey.length, 7, 3);
		byte[] nextRangeKey = DatabaseMapDictionaryDeep.nextRangeKey(prefixKey, prefixKey.length, 7, 3);

		if (Arrays.equals(prefixKey, new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF})) {
			Assertions.assertArrayEquals(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, nextRangeKey);
		} else {
			long biPrefix = 0;
			var s = 0;
			for (int i = prefixKey.length - 1; i >= 0; i--) {
				biPrefix += ((long) (prefixKey[i] & 0xFF)) << s;
				s += Byte.SIZE;
			}
			var nrPrefix = Arrays.copyOf(nextRangeKey, prefixKey.length);

			long biNextPrefix = 0;
			s = 0;
			for (int i = prefixKey.length - 1; i >= 0; i--) {
				biNextPrefix += ((long) (nrPrefix[i] & 0xFF)) << s;
				s += Byte.SIZE;
			}
			Assertions.assertEquals(biPrefix + 1, biNextPrefix);
			Assertions.assertArrayEquals(
					new byte[7 + 3],
					Arrays.copyOfRange(nextRangeKey, prefixKey.length, prefixKey.length + 7 + 3)
			);
		}
	}
	@Test
	public void testNextRangeKeyWithSuffix() {
		testNextRangeKeyWithSuffix(new byte[] {0x00, 0x01, (byte) 0xFF}, new byte[] {0x00, 0x00, 0x00});
		testNextRangeKeyWithSuffix(new byte[] {0x00, 0x00, 0x01}, new byte[] {0x00, 0x00, 0x01});
		testNextRangeKeyWithSuffix(new byte[] {0x00, 0x00, 0x02}, new byte[] {0x00, 0x00, 0x02});
		testNextRangeKeyWithSuffix(new byte[] {0x00, 0x01, 0x02}, new byte[] {0x00, 0x01, 0x02});
		testNextRangeKeyWithSuffix(new byte[] {0x00, 0x00, (byte) 0xFF}, new byte[] {0x00, 0x00, (byte) 0xFF});
		testNextRangeKeyWithSuffix(new byte[] {0x00, 0x01, (byte) 0xFF}, new byte[] {0x00, 0x01, (byte) 0xFF});
		testNextRangeKeyWithSuffix(new byte[] {0x00, (byte) 0xFF, (byte) 0xFF}, new byte[] {0x00, (byte) 0xFF, (byte) 0xFF});
		testNextRangeKeyWithSuffix(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
		testNextRangeKeyWithSuffix(new byte[] {(byte) 0xFF, (byte) 0, (byte) 0xFF}, new byte[] {(byte) 0xFF, (byte) 0, (byte) 0xFF});
		testNextRangeKeyWithSuffix(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0}, new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0});
	}

	public void testNextRangeKeyWithSuffix(byte[] prefixKey, byte[] suffixKey) {

		byte[] firstRangeKey = DatabaseMapDictionaryDeep.firstRangeKey(prefixKey, suffixKey, prefixKey.length, 3, 7);
		byte[] nextRangeKey = DatabaseMapDictionaryDeep.nextRangeKey(prefixKey, suffixKey, prefixKey.length, 3, 7);

		if (Arrays.equals(prefixKey, new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}) && Arrays.equals(suffixKey, new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF})) {
			Assertions.assertArrayEquals(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0, 0, 0, 0, 0}, nextRangeKey);
		} else {
			long biPrefix = 0;
			var s = 0;
			for (int i = (suffixKey.length) - 1; i >= 0; i--) {
				biPrefix += ((long) (suffixKey[i] & 0xFF)) << s;
				s += Byte.SIZE;
			}
			for (int i = (prefixKey.length) - 1; i >= 0; i--) {
				biPrefix += ((long) (prefixKey[i] & 0xFF)) << s;
				s += Byte.SIZE;
			}
			var nrPrefix = Arrays.copyOf(nextRangeKey, prefixKey.length + suffixKey.length);

			long biNextPrefix = 0;
			s = 0;
			for (int i = (prefixKey.length + suffixKey.length) - 1; i >= 0; i--) {
				biNextPrefix += ((long) (nrPrefix[i] & 0xFF)) << s;
				s += Byte.SIZE;
			}
			Assertions.assertEquals(biPrefix + 1, biNextPrefix);
			Assertions.assertArrayEquals(
					new byte[7],
					Arrays.copyOfRange(nextRangeKey, prefixKey.length + suffixKey.length, prefixKey.length + suffixKey.length + 7)
			);
		}
	}
}
