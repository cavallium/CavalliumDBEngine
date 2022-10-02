package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getProperty;
import static java.util.Objects.requireNonNull;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithValue;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithoutValue;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kNotExist;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;
import io.netty5.buffer.BufferComponent;
import io.netty5.buffer.DefaultBufferAllocators;
import it.cavallium.dbengine.database.LLUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Holder;
import org.rocksdb.KeyMayExist;
import org.rocksdb.KeyMayExist.KeyMayExistEnum;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public abstract class KeyMayExistGetter {

	private static final Logger LOG = LogManager.getLogger(KeyMayExistGetter.class);

	private static final boolean WORKAROUND_MAY_EXIST_FAKE_ZERO = parseBoolean(getProperty(
			"it.cavallium.dbengine.workaround_may_exist_fake_zero",
			"true"
	));
	private static final boolean STRICT_MAYEXIST_NO_VALUE = parseBoolean(getProperty(
			"it.cavallium.dbengine.mayexist.strict_no_value",
			"false"
	));
	private static final BufferAllocator OFF_HEAP_ALLOCATOR = DefaultBufferAllocators.offHeapAllocator();

	private final BufferAllocator bufferAllocator;
	private final boolean nettyDirect;

	public KeyMayExistGetter(BufferAllocator bufferAllocator, boolean nettyDirect) {
		this.bufferAllocator = bufferAllocator;
		this.nettyDirect = nettyDirect;
	}

	public final @Nullable Buffer get(@NotNull ReadOptions readOptions, Buffer key) throws RocksDBException {
		recordKeyBufferSize(key.readableBytes());
		if (nettyDirect) {
			return getDirect(readOptions, key);
		} else {
			return getHeap(readOptions, key);
		}
	}

	private Buffer getDirect(ReadOptions readOptions, Buffer key) throws RocksDBException {
		int readAttemptsCount = 0;
		// Get the key nio buffer to pass to RocksDB
		ByteBuffer keyNioBuffer;
		boolean mustCloseKey;
		{
			if (!LLUtils.isReadOnlyDirect(key)) {
				// If the nio buffer is not available, copy the netty buffer into a new direct buffer
				mustCloseKey = true;
				var directKey = OFF_HEAP_ALLOCATOR.allocate(key.readableBytes());
				key.copyInto(key.readerOffset(), directKey, 0, key.readableBytes());
				key = directKey;
			} else {
				mustCloseKey = false;
			}
			keyNioBuffer = ((BufferComponent) key).readableBuffer();
			assert keyNioBuffer.isDirect();
			assert keyNioBuffer.limit() == key.readableBytes();
		}

		try {
			// Create a direct result buffer because RocksDB works only with direct buffers
			var resultBuffer = bufferAllocator.allocate(INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES);
			try {
				assert resultBuffer.readerOffset() == 0;
				assert resultBuffer.writerOffset() == 0;
				var resultWritable = ((BufferComponent) resultBuffer).writableBuffer();

				var keyMayExist = keyMayExist(readOptions, keyNioBuffer.rewind(), resultWritable.clear());
				if (STRICT_MAYEXIST_NO_VALUE && keyMayExist.exists != kExistsWithValue && keyMayExist.valueLength != 0) {
					// Create a direct result buffer because RocksDB works only with direct buffers
					try (var realResultBuffer = bufferAllocator.allocate(INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES)) {
						var resultWritableF = resultWritable;
						var realResultWritable = ((BufferComponent) realResultBuffer).writableBuffer();
						var realSize = get(readOptions, keyNioBuffer.rewind(), realResultWritable);
						var hexFormat = HexFormat.ofDelimiter(" ");
						LOG.error(
								"KeyMayExist is {}, but value length is non-zero: {}! Disk value size is {}\nBytes from bloom cache:\n{}\nBytes from db:\n{}",
								() -> keyMayExist.exists,
								() -> keyMayExist.valueLength,
								() -> realSize,
								() -> {
									resultBuffer.writerOffset(resultWritableF.limit());
									return hexFormat.formatHex(LLUtils.toArray(resultBuffer));
								},
								() -> {
									realResultBuffer.writerOffset(realResultWritable.limit());
									return hexFormat.formatHex(LLUtils.toArray(realResultBuffer));
								}
						);
						var sliceKME = LLUtils.toArray(resultBuffer.copy(0, Math.min(resultWritableF.limit(), realSize)));
						var sliceDB = LLUtils.toArray(realResultBuffer.copy(0, Math.min(realResultWritable.limit(), realSize)));
						throw new RocksDBException(
								"KeyMayExist is " + keyMayExist.exists + ", but value length is non-zero: " + keyMayExist.valueLength
										+ "! Disk value size is " + realSize + ". The bloom cache partial value is "
										+ (Arrays.equals(sliceKME, sliceDB) ? "correct" : "corrupted"));
					}
				}
				KeyMayExistEnum keyMayExistState = keyMayExist.exists;
				int keyMayExistValueLength = keyMayExist.valueLength;
				// At the beginning, size reflects the expected size, then it becomes the real data size
				//noinspection SwitchStatementWithTooFewBranches
				int size = switch (keyMayExistState) {
					case kExistsWithValue -> keyMayExistValueLength;
					default -> -1;
				};
				boolean isKExistsWithoutValue = false;
				switch (keyMayExistState) {
					case kNotExist: {
						recordReadValueNotFoundWithBloomBufferSize(0);
						resultBuffer.close();
						return null;
					}
					// todo: kExistsWithValue is not reliable (read below),
					//  in some cases it should be treated as kExistsWithoutValue
					case kExistsWithValue:
					case kExistsWithoutValue: {
						if (keyMayExistState == kExistsWithoutValue) {
							isKExistsWithoutValue = true;
						} else if (WORKAROUND_MAY_EXIST_FAKE_ZERO) {
							// todo: "size == 0 || resultWritable.limit() == 0" is checked because keyMayExist is broken,
							//  and sometimes it returns an empty array, as if it exists
							if (size == 0 || resultWritable.limit() == 0) {
								isKExistsWithoutValue = true;
							}
						}
						if (isKExistsWithoutValue) {
							assert
									!STRICT_MAYEXIST_NO_VALUE || keyMayExistValueLength == 0 :
									"keyMayExist value length is " + keyMayExistValueLength + " instead of 0";
							resultWritable.clear();
							readAttemptsCount++;
							// real data size
							size = get(readOptions, keyNioBuffer.rewind(), resultWritable.clear());
							if (keyMayExistState == kExistsWithValue && size != keyMayExistValueLength) {
								throw new IllegalStateException("Bloom filter data is corrupted."
										+ " Bloom value size=" + keyMayExistState + ", Real value size=" + size);
							}
							if (size == RocksDB.NOT_FOUND) {
								resultBuffer.close();
								recordReadValueNotFoundWithMayExistBloomBufferSize(0);
								return null;
							}
						}
					}
					default: {
						// real data size
						assert size >= 0;
						if (size <= resultWritable.limit()) {
							if (isKExistsWithoutValue) {
								recordReadValueFoundWithBloomUncachedBufferSize(size);
							} else {
								recordReadValueFoundWithBloomCacheBufferSize(size);
							}
							assert size == resultWritable.limit();
							return resultBuffer.writerOffset(resultWritable.limit());
						} else {
							resultBuffer.ensureWritable(size);
							resultWritable = ((BufferComponent) resultBuffer).writableBuffer();
							assert resultBuffer.readerOffset() == 0;
							assert resultBuffer.writerOffset() == 0;

							readAttemptsCount++;
							size = get(readOptions, keyNioBuffer.rewind(), resultWritable.clear());
							if (size == RocksDB.NOT_FOUND) {
								recordReadValueNotFoundWithMayExistBloomBufferSize(0);
								resultBuffer.close();
								return null;
							}
							assert size == resultWritable.limit();
							if (isKExistsWithoutValue) {
								recordReadValueFoundWithBloomUncachedBufferSize(size);
							} else {
								recordReadValueFoundWithBloomCacheBufferSize(size);
							}
							return resultBuffer.writerOffset(resultWritable.limit());
						}
					}
				}
			} catch (Throwable t) {
				resultBuffer.close();
				throw t;
			}
		} finally {
			if (mustCloseKey) {
				key.close();
			}
			recordReadAttempts(readAttemptsCount);
		}
	}

	private Buffer getHeap(ReadOptions readOptions, Buffer key) throws RocksDBException {
		int readAttemptsCount = 0;
		try {
			byte[] keyArray = LLUtils.toArray(key);
			requireNonNull(keyArray);
			Holder<byte[]> data = new Holder<>();
			if (keyMayExist(readOptions, keyArray, data)) {
				// todo: "data.getValue().length > 0" is checked because keyMayExist is broken, and sometimes it
				//  returns an empty array, as if it exists
				if (data.getValue() != null && (!WORKAROUND_MAY_EXIST_FAKE_ZERO || data.getValue().length > 0)) {
					recordReadValueFoundWithBloomCacheBufferSize(data.getValue().length);
					return LLUtils.fromByteArray(bufferAllocator, data.getValue());
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
						return LLUtils.fromByteArray(bufferAllocator, result);
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

	protected abstract KeyMayExist keyMayExist(final ReadOptions readOptions, final ByteBuffer key, final ByteBuffer value);

	protected abstract boolean keyMayExist(final ReadOptions readOptions,
			final byte[] key,
			@Nullable final Holder<byte[]> valueHolder);

	protected abstract int get(final ReadOptions opt, final ByteBuffer key, final ByteBuffer value) throws RocksDBException;

	protected abstract byte[] get(final ReadOptions opt, final byte[] key) throws RocksDBException, IllegalArgumentException;

	protected abstract void recordReadValueNotFoundWithMayExistBloomBufferSize(int value);

	protected abstract void recordReadValueFoundWithBloomUncachedBufferSize(int value);

	protected abstract void recordReadValueFoundWithBloomCacheBufferSize(int value);

	protected abstract void recordReadAttempts(int value);

	protected abstract void recordReadValueNotFoundWithBloomBufferSize(int value);

	protected abstract void recordKeyBufferSize(int value);
}
