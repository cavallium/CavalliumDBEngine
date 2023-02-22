package it.cavallium.dbengine.tests;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import java.util.Arrays;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestRanges {

	@BeforeAll
	public static void beforeAll() {
	}

	@AfterAll
	public static void afterAll() {
	}

	@Test
	public void testNextRangeKey() {
		testNextRangeKey(new byte[] {0x00, 0x00, 0x00});
		testNextRangeKey(new byte[] {0x00, 0x00, 0x01});
		testNextRangeKey(new byte[] {0x00, 0x00, 0x02});
		testNextRangeKey(new byte[] {0x00, 0x01, 0x02});
		testNextRangeKey(new byte[] {0x00, 0x00, (byte) 0xFF});
		testNextRangeKey(new byte[] {0x00, 0x01, (byte) 0xFF});
		testNextRangeKey(new byte[] {0x00, (byte) 0xFF, (byte) 0xFF});
	}

	@Test
	public void testNextRangeKey2() {
		testNextRangeKey(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
		testNextRangeKey(new byte[] {(byte) 0xFF, (byte) 0, (byte) 0xFF});
		testNextRangeKey(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0});
	}

	public void testNextRangeKey(byte[] prefixKey) {
		byte[] firstRangeKey;
		Buf firstRangeKeyBuf = Buf.create(prefixKey.length);
		firstRangeKeyBuf.addElements(0, prefixKey);
		firstRangeKeyBuf = DatabaseMapDictionaryDeep.firstRangeKey(firstRangeKeyBuf, prefixKey.length, Buf.createZeroes(7 + 3));
		firstRangeKey = firstRangeKeyBuf.asArray();
		byte[] nextRangeKey;
		Buf nextRangeKeyBuf = Buf.create(prefixKey.length);
		nextRangeKeyBuf.addElements(0, prefixKey);
		nextRangeKeyBuf = DatabaseMapDictionaryDeep.nextRangeKey(nextRangeKeyBuf, prefixKey.length, Buf.createZeroes(7 + 3));
		nextRangeKey = nextRangeKeyBuf.asArray();

		if (Arrays.equals(prefixKey, new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF})) {
			org.assertj.core.api.Assertions
					.assertThat(nextRangeKey)
					.isEqualTo(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
							(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0});
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
}
