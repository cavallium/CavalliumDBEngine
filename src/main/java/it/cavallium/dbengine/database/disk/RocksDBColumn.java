package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.RepeatedElementList;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public sealed interface RocksDBColumn permits AbstractRocksDBColumn {

	@Nullable
	Send<Buffer> get(@NotNull ReadOptions readOptions, Send<Buffer> keySend,
			boolean existsAlmostCertainly) throws RocksDBException;

	boolean exists(@NotNull ReadOptions readOptions, Send<Buffer> keySend) throws RocksDBException;

	void put(@NotNull WriteOptions writeOptions, Send<Buffer> keyToReceive,
			Send<Buffer> valueToReceive) throws RocksDBException;

	@NotNull RocksIterator newIterator(@NotNull ReadOptions readOptions);

	@NotNull UpdateAtomicResult updateAtomic(@NotNull ReadOptions readOptions, @NotNull WriteOptions writeOptions,
			Send<Buffer> keySend, SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
			boolean existsAlmostCertainly, UpdateAtomicResultMode returnMode) throws RocksDBException, IOException;

	void delete(WriteOptions writeOptions, Send<Buffer> keySend) throws RocksDBException;

	void delete(WriteOptions writeOptions, byte[] key) throws RocksDBException;

	List<byte[]> multiGetAsList(ReadOptions resolveSnapshot, List<byte[]> keys) throws RocksDBException;

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
