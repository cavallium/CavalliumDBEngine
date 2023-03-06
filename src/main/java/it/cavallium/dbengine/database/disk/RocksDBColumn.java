package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.rocksdb.RocksIteratorObj;
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
	@NotNull RocksIteratorObj newRocksIterator(ReadOptions readOptions,
			LLRange range,
			boolean reverse) throws RocksDBException;

	default byte @Nullable [] get(@NotNull ReadOptions readOptions,
			byte[] key,
			boolean existsAlmostCertainly)
			throws RocksDBException {
		var result = this.get(readOptions, Buf.wrap(key));
		if (result == null) {
			return null;
		}
		return LLUtils.asArray(result);
	}

	@Nullable
	Buf get(@NotNull ReadOptions readOptions, Buf key) throws RocksDBException;

	boolean exists(@NotNull ReadOptions readOptions, Buf key) throws RocksDBException;

	boolean mayExists(@NotNull ReadOptions readOptions, Buf key) throws RocksDBException;

	void put(@NotNull WriteOptions writeOptions, Buf key, Buf value) throws RocksDBException;

	default void put(@NotNull WriteOptions writeOptions, byte[] key, byte[] value) throws RocksDBException {
		this.put(writeOptions, Buf.wrap(key), Buf.wrap(value));
	}

	@NotNull RocksIteratorObj newIterator(@NotNull ReadOptions readOptions, @Nullable Buf min, @Nullable Buf max);

	@NotNull UpdateAtomicResult updateAtomic(@NotNull ReadOptions readOptions,
			@NotNull WriteOptions writeOptions,
			Buf key,
			BinarySerializationFunction updater,
			UpdateAtomicResultMode returnMode) throws RocksDBException;

	void delete(WriteOptions writeOptions, Buf key) throws RocksDBException;

	void delete(WriteOptions writeOptions, byte[] key) throws RocksDBException;

	List<byte[]> multiGetAsList(ReadOptions readOptions, List<byte[]> keys) throws RocksDBException;

	void write(WriteOptions writeOptions, WriteBatch writeBatch) throws RocksDBException;

	void suggestCompactRange() throws RocksDBException;

	void compactRange(byte[] begin, byte[] end, CompactRangeOptions options) throws RocksDBException;

	void flush(FlushOptions options) throws RocksDBException;

	void flushWal(boolean sync) throws RocksDBException;

	long getLongProperty(String property) throws RocksDBException;

	long getNumEntries() throws RocksDBException;

	ColumnFamilyHandle getColumnFamilyHandle();

	MeterRegistry getMeterRegistry();

	boolean supportsTransactions();

	void forceCompaction(int volumeId) throws RocksDBException;
}
