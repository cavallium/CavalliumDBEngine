package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.MeterRegistry;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.rocksdb.RocksIteratorObj;
import it.cavallium.dbengine.database.disk.rocksdb.RocksObj;
import java.io.IOException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public sealed interface RocksDBColumn permits AbstractRocksDBColumn {

	/**
	 * This method should not modify or move the writerIndex/readerIndex of the buffers inside the range
	 */
	@NotNull RocksIteratorObj newRocksIterator(boolean allowNettyDirect,
			RocksObj<ReadOptions> readOptions,
			LLRange range,
			boolean reverse) throws RocksDBException;

	default byte @Nullable [] get(@NotNull RocksObj<ReadOptions> readOptions,
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
	Buffer get(@NotNull RocksObj<ReadOptions> readOptions, Buffer key) throws RocksDBException;

	boolean exists(@NotNull RocksObj<ReadOptions> readOptions, Buffer key) throws RocksDBException;

	boolean mayExists(@NotNull RocksObj<ReadOptions> readOptions, Buffer key) throws RocksDBException;

	void put(@NotNull RocksObj<WriteOptions> writeOptions, Buffer key, Buffer value) throws RocksDBException;

	default void put(@NotNull RocksObj<WriteOptions> writeOptions, byte[] key, byte[] value) throws RocksDBException {
		var allocator = getAllocator();
		try (var keyBuf = allocator.allocate(key.length)) {
			keyBuf.writeBytes(key);
			try (var valBuf = allocator.allocate(value.length)) {
				valBuf.writeBytes(value);

				this.put(writeOptions, keyBuf, valBuf);
			}
		}
	}

	@NotNull RocksIteratorObj newIterator(@NotNull RocksObj<ReadOptions> readOptions, @Nullable Buffer min, @Nullable Buffer max);

	@NotNull UpdateAtomicResult updateAtomic(@NotNull RocksObj<ReadOptions> readOptions,
			@NotNull RocksObj<WriteOptions> writeOptions,
			Buffer key,
			BinarySerializationFunction updater,
			UpdateAtomicResultMode returnMode) throws RocksDBException, IOException;

	void delete(RocksObj<WriteOptions> writeOptions, Buffer key) throws RocksDBException;

	void delete(RocksObj<WriteOptions> writeOptions, byte[] key) throws RocksDBException;

	List<byte[]> multiGetAsList(RocksObj<ReadOptions> readOptions, List<byte[]> keys) throws RocksDBException;

	void write(RocksObj<WriteOptions> writeOptions, WriteBatch writeBatch) throws RocksDBException;

	void suggestCompactRange() throws RocksDBException;

	void compactRange(byte[] begin, byte[] end, RocksObj<CompactRangeOptions> options) throws RocksDBException;

	void flush(RocksObj<FlushOptions> options) throws RocksDBException;

	void flushWal(boolean sync) throws RocksDBException;

	long getLongProperty(String property) throws RocksDBException;

	RocksObj<ColumnFamilyHandle> getColumnFamilyHandle();

	BufferAllocator getAllocator();

	MeterRegistry getMeterRegistry();

	boolean supportsTransactions();

	void forceCompaction(int volumeId) throws RocksDBException;
}
