package it.cavallium.dbengine;

import static java.util.Map.entry;

import io.netty5.buffer.BufferAllocator;
import it.cavallium.dbengine.database.disk.KeyMayExistGetter;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.rocksdb.Holder;
import org.rocksdb.KeyMayExist;
import org.rocksdb.KeyMayExist.KeyMayExistEnum;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
//todo:
public class TestGetter {

	private static Map<ByteList, ByteList> db = Map.ofEntries(
			entry(bytes("cached-partially"), bytes("123456789")),
			entry(bytes("cached-totally"), bytes("ciaov")),
			entry(bytes("cached-without-value"), bytes("ciaov")),
			entry(bytes("ciao3"), bytes("ciaov"))
	);

	record KeyMayExistResult(KeyMayExist.KeyMayExistEnum keyMayExistEnum, int size, ByteList cachedValue) {}

	private static Map<ByteList, KeyMayExistResult> cache = Map.ofEntries(
			entry(bytes("cached-partially"), new KeyMayExistResult(KeyMayExistEnum.kExistsWithoutValue, 9, bytes("12345678"))),
			entry(bytes("cached-totally"), new KeyMayExistResult(KeyMayExistEnum.kExistsWithValue, 5, bytes("ciaov"))),
			entry(bytes("cached-without-value"), new KeyMayExistResult(KeyMayExistEnum.kExistsWithoutValue, 0, bytes("ciaov")))
	);

	private static ByteList bytes(String text) {
		return ByteList.of(text.getBytes(StandardCharsets.UTF_8));
	}

	private static String text(ByteList bytes) {
		return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
	}

	public KeyMayExistGetter getter = new KeyMayExistGetter(BufferAllocator.offHeapUnpooled(), true) {
		@Override
		protected KeyMayExist keyMayExist(ReadOptions readOptions, ByteBuffer key, ByteBuffer value) {
			return null;
		}

		@Override
		protected boolean keyMayExist(ReadOptions readOptions, byte[] key, @Nullable Holder<byte[]> valueHolder) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected int get(ReadOptions opt, ByteBuffer key, ByteBuffer value) throws RocksDBException {
			return 0;
		}

		@Override
		protected byte[] get(ReadOptions opt, byte[] key) throws RocksDBException, IllegalArgumentException {
			throw new UnsupportedOperationException();
		}

		@Override
		protected void recordReadValueNotFoundWithMayExistBloomBufferSize(int value) {

		}

		@Override
		protected void recordReadValueFoundWithBloomUncachedBufferSize(int value) {

		}

		@Override
		protected void recordReadValueFoundWithBloomCacheBufferSize(int value) {

		}

		@Override
		protected void recordReadAttempts(int value) {

		}

		@Override
		protected void recordReadValueNotFoundWithBloomBufferSize(int value) {

		}

		@Override
		protected void recordKeyBufferSize(int value) {

		}
	};

	@Test
	public void testSimpleGet() {

	}
}
