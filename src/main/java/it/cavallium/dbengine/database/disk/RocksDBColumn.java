package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
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

	default byte @Nullable [] get(@NotNull ReadOptions readOptions,
			byte[] key,
			boolean existsAlmostCertainly)
			throws RocksDBException {
		var allocator = getAllocator();
		try (var keyBuf = allocator.allocate(key.length)) {
			keyBuf.writeBytes(key);
			var result = this.get(readOptions, keyBuf.send(), existsAlmostCertainly);
			if (result == null) {
				return null;
			}
			try (var resultBuf = result.receive()) {
				return LLUtils.toArray(resultBuf);
			}
		}
	}

	@Nullable
	Send<Buffer> get(@NotNull ReadOptions readOptions, Send<Buffer> keySend,
			boolean existsAlmostCertainly) throws RocksDBException;

	boolean exists(@NotNull ReadOptions readOptions, Send<Buffer> keySend) throws RocksDBException;

	void put(@NotNull WriteOptions writeOptions, Send<Buffer> keyToReceive,
			Send<Buffer> valueToReceive) throws RocksDBException;

	default void put(@NotNull WriteOptions writeOptions, byte[] key, byte[] value)
			throws RocksDBException {
		var allocator = getAllocator();
		try (var keyBuf = allocator.allocate(key.length)) {
			keyBuf.writeBytes(key);
			try (var valBuf = allocator.allocate(value.length)) {
				valBuf.writeBytes(value);

				this.put(writeOptions, keyBuf.send(), valBuf.send());
			}
		}
	}

	@NotNull RocksIterator newIterator(@NotNull ReadOptions readOptions);

	@NotNull UpdateAtomicResult updateAtomic(@NotNull ReadOptions readOptions, @NotNull WriteOptions writeOptions,
			Send<Buffer> keySend, SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
			boolean existsAlmostCertainly, UpdateAtomicResultMode returnMode) throws RocksDBException, IOException;

	void delete(WriteOptions writeOptions, Send<Buffer> keySend) throws RocksDBException;

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
