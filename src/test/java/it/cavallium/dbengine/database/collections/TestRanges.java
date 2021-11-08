package it.cavallium.dbengine.database.collections;

import io.net5.buffer.Unpooled;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import it.cavallium.dbengine.database.LLUtils;
import java.util.Arrays;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static io.net5.buffer.Unpooled.*;

public class TestRanges {

	private static BufferAllocator alloc;

	@BeforeAll
	public static void beforeAll() {
		alloc = BufferAllocator.offHeapPooled();
	}

	@AfterAll
	public static void afterAll() {
		alloc = BufferAllocator.offHeapPooled();
	}

	@Test
	public void testDirectBuffer() {
		Assertions.assertTrue(wrappedBuffer(Unpooled.directBuffer(10, 10), Unpooled.buffer(10, 10)).isDirect());
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
		Buffer firstRangeKeyBuf = alloc.allocate(prefixKey.length).writeBytes(prefixKey);
		try (firstRangeKeyBuf) {
			DatabaseMapDictionaryDeep.firstRangeKey(firstRangeKeyBuf, prefixKey.length, 7, 3);
			firstRangeKey = LLUtils.toArray(firstRangeKeyBuf);
		}
		byte[] nextRangeKey;
		Buffer nextRangeKeyBuf = alloc.allocate(prefixKey.length).writeBytes(prefixKey);
		try (nextRangeKeyBuf) {
			DatabaseMapDictionaryDeep.nextRangeKey(nextRangeKeyBuf, prefixKey.length, 7, 3);
			nextRangeKey = LLUtils.toArray(nextRangeKeyBuf);
		}

		if (Arrays.equals(prefixKey, new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF})) {
			Assertions.assertArrayEquals(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0}, nextRangeKey);
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
