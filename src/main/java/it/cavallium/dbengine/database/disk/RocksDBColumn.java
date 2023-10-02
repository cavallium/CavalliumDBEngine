package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.rocksdb.LLReadOptions;
import it.cavallium.dbengine.database.disk.rocksdb.LLWriteOptions;
import it.cavallium.dbengine.database.disk.rocksdb.RocksIteratorObj;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

public sealed interface RocksDBColumn permits AbstractRocksDBColumn {

	/**
	 * This method should not modify or move the writerIndex/readerIndex of the buffers inside the range
	 */
	@NotNull RocksIteratorObj newRocksIterator(LLReadOptions readOptions,
			LLRange range,
			boolean reverse) throws RocksDBException;

	default byte @Nullable [] get(@NotNull LLReadOptions readOptions,
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
	Buf get(@NotNull LLReadOptions readOptions, Buf key) throws RocksDBException;

	boolean exists(@NotNull LLReadOptions readOptions, Buf key) throws RocksDBException;

	boolean mayExists(@NotNull LLReadOptions readOptions, Buf key);

	void put(@NotNull LLWriteOptions writeOptions, Buf key, Buf value) throws RocksDBException;

	default void put(@NotNull LLWriteOptions writeOptions, byte[] key, byte[] value) throws RocksDBException {
		this.put(writeOptions, Buf.wrap(key), Buf.wrap(value));
	}

	@NotNull RocksIteratorObj newIterator(@NotNull LLReadOptions readOptions, @Nullable Buf min, @Nullable Buf max);

	Stream<RocksDBFile> getAllLiveFiles() throws RocksDBException;

	@NotNull UpdateAtomicResult updateAtomic(@NotNull LLReadOptions readOptions,
			@NotNull LLWriteOptions writeOptions,
			Buf key,
			SerializationFunction<@Nullable Buf, @Nullable Buf> updater,
			UpdateAtomicResultMode returnMode);

	void delete(LLWriteOptions writeOptions, Buf key) throws RocksDBException;

	void delete(LLWriteOptions writeOptions, byte[] key) throws RocksDBException;

	List<byte[]> multiGetAsList(LLReadOptions readOptions, List<byte[]> keys) throws RocksDBException;

	void write(LLWriteOptions writeOptions, WriteBatch writeBatch) throws RocksDBException;

	void suggestCompactRange() throws RocksDBException;

	void compactRange(byte[] begin, byte[] end, CompactRangeOptions options) throws RocksDBException;

	void flush(FlushOptions options) throws RocksDBException;

	void flushWal(boolean sync) throws RocksDBException;

	long getLongProperty(String property) throws RocksDBException;

	long getNumEntries() throws RocksDBException;

	ColumnFamilyHandle getColumnFamilyHandle();

	MeterRegistry getMeterRegistry();

	boolean supportsTransactions();

	void forceCompaction(int volumeId);
}
