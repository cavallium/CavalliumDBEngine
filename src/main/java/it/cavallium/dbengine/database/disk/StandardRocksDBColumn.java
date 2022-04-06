package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;

import io.micrometer.core.instrument.MeterRegistry;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

public final class StandardRocksDBColumn extends AbstractRocksDBColumn<RocksDB> {

	public StandardRocksDBColumn(RocksDB db,
			boolean nettyDirect,
			BufferAllocator alloc,
			String dbName,
			ColumnFamilyHandle cfh, MeterRegistry meterRegistry) {
		super(db, nettyDirect, alloc, dbName, cfh, meterRegistry);
	}

	@Override
	protected boolean commitOptimistically(Transaction tx) {
		throw new UnsupportedOperationException("Transactions not supported");
	}

	@Override
	protected Transaction beginTransaction(@NotNull WriteOptions writeOptions) {
		throw new UnsupportedOperationException("Transactions not supported");
	}

	@Override
	public @NotNull UpdateAtomicResult updateAtomicImpl(@NotNull ReadOptions readOptions,
			@NotNull WriteOptions writeOptions,
			Buffer key,
			BinarySerializationFunction updater,
			UpdateAtomicResultMode returnMode) throws IOException {
		long initNanoTime = System.nanoTime();
		try {
			@Nullable Buffer prevData = this.get(readOptions, key);
			try (prevData) {
				if (logger.isTraceEnabled()) {
					logger.trace(MARKER_ROCKSDB,
							"Reading {}: {} (before update)",
							LLUtils.toStringSafe(key),
							LLUtils.toStringSafe(prevData)
					);
				}

				Buffer prevDataToSendToUpdater;
				if (prevData != null) {
					prevDataToSendToUpdater = prevData.copy().makeReadOnly();
				} else {
					prevDataToSendToUpdater = null;
				}

				@Nullable Buffer newData = applyUpdateAndCloseIfNecessary(updater, prevDataToSendToUpdater);
				try (newData) {
					boolean changed;
					assert newData == null || newData.isAccessible();
					if (logger.isTraceEnabled()) {
						logger.trace(MARKER_ROCKSDB,
								"Updating {}. previous data: {}, updated data: {}",
								LLUtils.toStringSafe(key),
								LLUtils.toStringSafe(prevData),
								LLUtils.toStringSafe(newData)
						);
					}
					if (prevData != null && newData == null) {
						if (logger.isTraceEnabled()) {
							logger.trace(MARKER_ROCKSDB, "Deleting {} (after update)", LLUtils.toStringSafe(key));
						}
						this.delete(writeOptions, key);
						changed = true;
					} else if (newData != null && (prevData == null || !LLUtils.equals(prevData, newData))) {
						if (logger.isTraceEnabled()) {
							logger.trace(MARKER_ROCKSDB,
									"Writing {}: {} (after update)",
									LLUtils.toStringSafe(key),
									LLUtils.toStringSafe(newData)
							);
						}
						Buffer dataToPut;
						if (returnMode == UpdateAtomicResultMode.CURRENT) {
							dataToPut = newData.copy();
						} else {
							dataToPut = newData;
						}
						try {
							this.put(writeOptions, key, dataToPut);
							changed = true;
						} finally {
							if (dataToPut != newData) {
								dataToPut.close();
							}
						}
					} else {
						changed = false;
					}
					recordAtomicUpdateTime(changed, prevData != null, newData != null, initNanoTime);
					return switch (returnMode) {
						case NOTHING -> {
							yield RESULT_NOTHING;
						}
						case CURRENT -> {
							yield new UpdateAtomicResultCurrent(newData != null ? newData.send() : null);
						}
						case PREVIOUS -> {
							yield new UpdateAtomicResultPrevious(prevData != null ? prevData.send() : null);
						}
						case BINARY_CHANGED -> new UpdateAtomicResultBinaryChanged(changed);
						case DELTA -> new UpdateAtomicResultDelta(LLDelta
								.of(prevData != null ? prevData.send() : null, newData != null ? newData.send() : null)
								.send());
					};
				}
			}
		} catch (Throwable ex) {
			throw new IOException("Failed to update key " + LLUtils.toStringSafe(key), ex);
		}
	}

	@Override
	public boolean supportsTransactions() {
		return false;
	}
}
