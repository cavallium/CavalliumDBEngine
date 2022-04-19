package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.MeterRegistry;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.io.IOException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public sealed interface RocksDBColumn permits AbstractRocksDBColumn {

	/**
	 * This method should not modify or move the writerIndex/readerIndex of the buffers inside the range
	 */
	@NotNull
	RocksIteratorTuple getRocksIterator(boolean allowNettyDirect,
			ReadOptions readOptions,
			LLRange range,
			boolean reverse) throws RocksDBException;

	default byte @Nullable [] get(@NotNull ReadOptions readOptions,
			byte[] key,
			boolean existsAlmostCertainly)
			throws RocksDBException {
		var allocator = getAllocator();
		try (var keyBuf = allocator.allocate(key.length)) {
			keyBuf.writeBytes(key);
			try (var result = this.get(readOptions, keyBuf)) {
				if (result == null) {
					return null;
				}
				return LLUtils.toArray(result);
			}
		}
	}

	@Nullable
	Buffer get(@NotNull ReadOptions readOptions, Buffer key) throws RocksDBException;

	boolean exists(@NotNull ReadOptions readOptions, Buffer key) throws RocksDBException;

	boolean mayExists(@NotNull ReadOptions readOptions, Buffer key) throws RocksDBException;

	void put(@NotNull WriteOptions writeOptions, Buffer key, Buffer value) throws RocksDBException;

	default void put(@NotNull WriteOptions writeOptions, byte[] key, byte[] value)
			throws RocksDBException {
		var allocator = getAllocator();
		try (var keyBuf = allocator.allocate(key.length)) {
			keyBuf.writeBytes(key);
			try (var valBuf = allocator.allocate(value.length)) {
				valBuf.writeBytes(value);

				this.put(writeOptions, keyBuf, valBuf);
			}
		}
	}

	@NotNull RocksDBIterator newIterator(@NotNull ReadOptions readOptions);

	@NotNull UpdateAtomicResult updateAtomic(@NotNull ReadOptions readOptions, @NotNull WriteOptions writeOptions,
			Buffer key, BinarySerializationFunction updater,
			UpdateAtomicResultMode returnMode) throws RocksDBException, IOException;

	void delete(WriteOptions writeOptions, Buffer key) throws RocksDBException;

	void delete(WriteOptions writeOptions, byte[] key) throws RocksDBException;

	List<byte[]> multiGetAsList(ReadOptions readOptions, List<byte[]> keys) throws RocksDBException;

	void write(WriteOptions writeOptions, WriteBatch writeBatch) throws RocksDBException;

	void suggestCompactRange() throws RocksDBException;

	void compactRange(byte[] begin, byte[] end, CompactRangeOptions options) throws RocksDBException;

	void flush(FlushOptions options) throws RocksDBException;

	void flushWal(boolean sync) throws RocksDBException;

	long getLongProperty(String property) throws RocksDBException;

	ColumnFamilyHandle getColumnFamilyHandle();

	BufferAllocator getAllocator();

	MeterRegistry getMeterRegistry();

	boolean supportsTransactions();
}
