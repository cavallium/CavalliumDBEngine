package it.cavallium.dbengine.database.disk;

import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getProperty;
import static java.util.Objects.requireNonNull;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.rocksdb.LLReadOptions;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.Holder;
import org.rocksdb.KeyMayExist;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;

public abstract class KeyMayExistGetter {

	private static final Logger LOG = LogManager.getLogger(KeyMayExistGetter.class);

	private static final boolean WORKAROUND_MAY_EXIST_FAKE_ZERO = parseBoolean(getProperty(
			"it.cavallium.dbengine.workaround_may_exist_fake_zero",
			"false"
	));
	private static final boolean STRICT_MAYEXIST_NO_VALUE = parseBoolean(getProperty(
			"it.cavallium.dbengine.mayexist.strict_no_value",
			"false"
	));

	public KeyMayExistGetter() {
	}

	public final @Nullable Buf get(@NotNull LLReadOptions readOptions, Buf key) throws RocksDBException {
		recordKeyBufferSize(key.size());
		return getHeap(readOptions, key);
	}

	private Buf getHeap(LLReadOptions readOptions, Buf key) throws RocksDBException {
		int readAttemptsCount = 0;
		try {
			byte[] keyArray = LLUtils.asArray(key);
			requireNonNull(keyArray);
			Holder<byte[]> data = new Holder<>();
			if (keyMayExist(readOptions, keyArray, data)) {
				// todo: "data.getValue().length > 0" is checked because keyMayExist is broken, and sometimes it
				//  returns an empty array, as if it exists
				if (data.getValue() != null && (!WORKAROUND_MAY_EXIST_FAKE_ZERO || data.getValue().length > 0)) {
					recordReadValueFoundWithBloomCacheBufferSize(data.getValue().length);
					return LLUtils.asByteList(data.getValue());
				} else {
					readAttemptsCount++;
					byte[] result = get(readOptions, keyArray);
					if (result == null) {
						if (data.getValue() != null) {
							recordReadValueNotFoundWithBloomBufferSize(0);
						} else {
							recordReadValueNotFoundWithMayExistBloomBufferSize(0);
						}
						return null;
					} else {
						recordReadValueFoundWithBloomUncachedBufferSize(0);
						return LLUtils.asByteList(result);
					}
				}
			} else {
				recordReadValueNotFoundWithBloomBufferSize(0);
				return null;
			}
		} finally {
			recordReadAttempts(readAttemptsCount);
		}
	}

	protected abstract KeyMayExist keyMayExist(final LLReadOptions readOptions, final ByteBuffer key, final ByteBuffer value);

	protected abstract boolean keyMayExist(final LLReadOptions readOptions,
			final byte[] key,
			@Nullable final Holder<byte[]> valueHolder);

	protected abstract int get(final LLReadOptions opt, final ByteBuffer key, final ByteBuffer value) throws RocksDBException;

	protected abstract byte[] get(final LLReadOptions opt, final byte[] key) throws RocksDBException, IllegalArgumentException;

	protected abstract void recordReadValueNotFoundWithMayExistBloomBufferSize(int value);

	protected abstract void recordReadValueFoundWithBloomUncachedBufferSize(int value);

	protected abstract void recordReadValueFoundWithBloomCacheBufferSize(int value);

	protected abstract void recordReadAttempts(int value);

	protected abstract void recordReadValueNotFoundWithBloomBufferSize(int value);

	protected abstract void recordKeyBufferSize(int value);
}
