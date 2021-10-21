package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.MemoryManager;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.lucene.ExponentialPageLimits;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status.Code;
import org.rocksdb.Transaction;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import reactor.core.scheduler.Schedulers;

public final class OptimisticRocksDBColumn extends AbstractRocksDBColumn<OptimisticTransactionDB> {

	public OptimisticRocksDBColumn(OptimisticTransactionDB db,
			DatabaseOptions databaseOptions,
			BufferAllocator alloc,
			ColumnFamilyHandle cfh) {
		super(db, databaseOptions, alloc, cfh);
	}

	@Override
	protected boolean commitOptimistically(Transaction tx) throws RocksDBException {
		try {
			tx.commit();
			return true;
		} catch (RocksDBException ex) {
			var status = ex.getStatus() != null ? ex.getStatus().getCode() : Code.Ok;
			if (status == Code.Busy || status == Code.TryAgain) {
				return false;
			}
			throw ex;
		}
	}

	@Override
	protected Transaction beginTransaction(@NotNull WriteOptions writeOptions) {
		return getDb().beginTransaction(writeOptions);
	}

	@Override
	public void write(WriteOptions writeOptions, WriteBatch writeBatch) throws RocksDBException {
		getDb().write(writeOptions, writeBatch);
	}

	@Override
	public @NotNull UpdateAtomicResult updateAtomic(@NotNull ReadOptions readOptions,
			@NotNull WriteOptions writeOptions,
			Send<Buffer> keySend,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Send<Buffer>> updater,
			boolean existsAlmostCertainly,
			UpdateAtomicResultMode returnMode) throws IOException, RocksDBException {
		try (Buffer key = keySend.receive()) {
			var cfh = getCfh();
			var keyArray = LLUtils.toArray(key);
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called update in a nonblocking thread");
			}
			try (var tx = beginTransaction(writeOptions)) {
				boolean committedSuccessfully;
				int retries = 0;
				ExponentialPageLimits retryTime = null;
				Send<Buffer> sentPrevData;
				Send<Buffer> sentCurData;
				boolean changed;
				do {
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
							try (var newDataToReceive = updater.apply(sentData)) {
								if (newDataToReceive != null) {
									newData = newDataToReceive.receive();
								} else {
									newData = null;
								}
							}
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
								committedSuccessfully = commitOptimistically(tx);
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
								committedSuccessfully = commitOptimistically(tx);
							} else {
								changed = false;
								committedSuccessfully = true;
								tx.rollback();
							}
							sentPrevData = prevData == null ? null : prevData.send();
							sentCurData = newData == null ? null : newData.send();
							if (!committedSuccessfully) {
								tx.undoGetForUpdate(cfh, keyArray);
								if (sentPrevData != null) {
									sentPrevData.close();
								}
								if (sentCurData != null) {
									sentCurData.close();
								}
								retries++;
								if (retries >= 100 && retries % 100 == 0) {
									logger.warn(MARKER_ROCKSDB, "Failed optimistic transaction {} (update):"
											+ " waiting 5ms before retrying for the {} time", LLUtils.toStringSafe(key), retries);
								} else if (logger.isDebugEnabled(MARKER_ROCKSDB)) {
									logger.debug(MARKER_ROCKSDB, "Failed optimistic transaction {} (update):"
											+ " waiting 5ms before retrying", LLUtils.toStringSafe(key));
								}
								if (retries == 1) {
									retryTime = new ExponentialPageLimits(0, 5, 2000);
								}
								long retryMs = retryTime.getPageLimit(retries);
								// +- 20%
								retryMs = retryMs + (long) (retryMs * 0.2d * ThreadLocalRandom.current().nextDouble(-1.0d, 1.0d));
								// Wait for 5ms
								try {
									Thread.sleep(retryMs);
								} catch (InterruptedException e) {
									throw new RocksDBException("Interrupted");
								}
							}
						}
					}
				} while (!committedSuccessfully);
				if (retries > 5) {
					logger.warn(MARKER_ROCKSDB, "Took {} retries to update key {}", retries, LLUtils.toStringSafe(key));
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
