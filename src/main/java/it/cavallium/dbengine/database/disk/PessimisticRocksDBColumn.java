package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;

import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.MemoryManager;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteOptions;
import reactor.core.scheduler.Schedulers;

public final class PessimisticRocksDBColumn extends AbstractRocksDBColumn<TransactionDB> {

	private static final TransactionOptions DEFAULT_TX_OPTIONS = new TransactionOptions();

	public PessimisticRocksDBColumn(TransactionDB db,
			DatabaseOptions databaseOptions,
			BufferAllocator alloc,
			ColumnFamilyHandle cfh, MeterRegistry meterRegistry) {
		super(db, databaseOptions, alloc, cfh, meterRegistry);
	}

	@Override
	protected boolean commitOptimistically(Transaction tx) throws RocksDBException {
		tx.commit();
		return true;
	}

	@Override
	protected Transaction beginTransaction(@NotNull WriteOptions writeOptions) {
		return getDb().beginTransaction(writeOptions, DEFAULT_TX_OPTIONS);
	}

	@Override
	public @NotNull UpdateAtomicResult updateAtomic(@NotNull ReadOptions readOptions,
			@NotNull WriteOptions writeOptions,
			Send<Buffer> keySend,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
			boolean existsAlmostCertainly,
			UpdateAtomicResultMode returnMode) throws IOException, RocksDBException {
		try (Buffer key = keySend.receive()) {
			var cfh = getCfh();
			var keyArray = LLUtils.toArray(key);
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called update in a nonblocking thread");
			}
			try (var tx = beginTransaction(writeOptions)) {
				Send<Buffer> sentPrevData;
				Send<Buffer> sentCurData;
				boolean changed;
				var prevDataArray = tx.getForUpdate(readOptions, cfh, keyArray, true);
				if (logger.isTraceEnabled()) {
					logger.trace(MARKER_ROCKSDB,
							"Reading {}: {} (before update)",
							LLUtils.toStringSafe(key),
							LLUtils.toStringSafe(prevDataArray)
					);
				}
				Buffer prevData;
				if (prevDataArray != null) {
					prevData = MemoryManager.unsafeWrap(prevDataArray);
				} else {
					prevData = null;
				}
				try (prevData) {
					Buffer prevDataToSendToUpdater;
					if (prevData != null) {
						prevDataToSendToUpdater = prevData.copy();
					} else {
						prevDataToSendToUpdater = null;
					}

					@Nullable Buffer newData;
					try (var sentData = prevDataToSendToUpdater == null ? null : prevDataToSendToUpdater.send()) {
						newData = updater.apply(sentData);
					}
					try (newData) {
						var newDataArray = newData == null ? null : LLUtils.toArray(newData);
						if (logger.isTraceEnabled()) {
							logger.trace(MARKER_ROCKSDB,
									"Updating {}. previous data: {}, updated data: {}",
									LLUtils.toStringSafe(key),
									LLUtils.toStringSafe(prevDataArray),
									LLUtils.toStringSafe(newDataArray)
							);
						}
						if (prevData != null && newData == null) {
							if (logger.isTraceEnabled()) {
								logger.trace(MARKER_ROCKSDB, "Deleting {} (after update)", LLUtils.toStringSafe(key));
							}
							tx.delete(cfh, keyArray, true);
							changed = true;
							tx.commit();
						} else if (newData != null && (prevData == null || !LLUtils.equals(prevData, newData))) {
							if (logger.isTraceEnabled()) {
								logger.trace(MARKER_ROCKSDB,
										"Writing {}: {} (after update)",
										LLUtils.toStringSafe(key),
										LLUtils.toStringSafe(newData)
								);
							}
							tx.put(cfh, keyArray, newDataArray);
							changed = true;
							tx.commit();
						} else {
							changed = false;
							tx.rollback();
						}
						sentPrevData = prevData == null ? null : prevData.send();
						sentCurData = newData == null ? null : newData.send();
					}
				}
				return switch (returnMode) {
					case NOTHING -> {
						if (sentPrevData != null) {
							sentPrevData.close();
						}
						if (sentCurData != null) {
							sentCurData.close();
						}
						yield RESULT_NOTHING;
					}
					case CURRENT -> {
						if (sentPrevData != null) {
							sentPrevData.close();
						}
						yield new UpdateAtomicResultCurrent(sentCurData);
					}
					case PREVIOUS -> {
						if (sentCurData != null) {
							sentCurData.close();
						}
						yield new UpdateAtomicResultPrevious(sentPrevData);
					}
					case BINARY_CHANGED -> new UpdateAtomicResultBinaryChanged(changed);
					case DELTA -> new UpdateAtomicResultDelta(LLDelta.of(sentPrevData, sentCurData).send());
				};
			}
		}
	}

	@Override
	public boolean supportsTransactions() {
		return true;
	}
}
