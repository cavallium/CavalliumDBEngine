package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.ExponentialPageLimits;
import it.cavallium.dbengine.utils.DBException;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.StampedLock;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status.Code;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public final class OptimisticRocksDBColumn extends AbstractRocksDBColumn<OptimisticTransactionDB> {

	private static final boolean ALWAYS_PRINT_OPTIMISTIC_RETRIES = false;

	private final DistributionSummary optimisticAttempts;

	public OptimisticRocksDBColumn(OptimisticTransactionDB db,
			String databaseName,
			ColumnFamilyHandle cfh,
			MeterRegistry meterRegistry,
			StampedLock closeLock) {
		super(db, databaseName, cfh, meterRegistry, closeLock);
		this.optimisticAttempts = DistributionSummary
				.builder("db.optimistic.attempts.distribution")
				.publishPercentiles(0.2, 0.5, 0.95)
				.baseUnit("times")
				.scale(1)
				.publishPercentileHistogram()
				.tags("db.name", databaseName, "db.column", columnName)
				.register(meterRegistry);
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
	protected Transaction beginTransaction(@NotNull WriteOptions writeOptions,
			TransactionOptions txOpts) {
		return getDb().beginTransaction(writeOptions);
	}

	@Override
	public void write(WriteOptions writeOptions, WriteBatch writeBatch) throws RocksDBException {
		getDb().write(writeOptions, writeBatch);
	}

	@Override
	public @NotNull UpdateAtomicResult updateAtomicImpl(@NotNull ReadOptions readOptions,
			@NotNull WriteOptions writeOptions,
			Buf key,
			BinarySerializationFunction updater,
			UpdateAtomicResultMode returnMode) {
		long initNanoTime = System.nanoTime();
		try {
			var cfh = getCfh();
			var keyArray = LLUtils.asArray(key);
			if (LLUtils.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called update in a nonblocking thread");
			}
			try (var txOpts = new TransactionOptions();
					var tx = beginTransaction(writeOptions, txOpts)) {
				boolean committedSuccessfully;
				int retries = 0;
				ExponentialPageLimits retryTime = null;
				Buf prevData = null;
				Buf newData = null;
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
					if (prevDataArray != null) {
						prevData = Buf.wrap(prevDataArray);
						prevDataArray = null;
					} else {
						prevData = null;
					}
					Buf prevDataToSendToUpdater;
					if (prevData != null) {
						prevDataToSendToUpdater = prevData.copy();
					} else {
						prevDataToSendToUpdater = null;
					}
					newData = updater.apply(prevDataToSendToUpdater);
					var newDataArray = newData == null ? null : LLUtils.asArray(newData);
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
					if (!committedSuccessfully) {
						tx.undoGetForUpdate(cfh, keyArray);
						tx.rollback();
						retries++;

						if (retries == 1) {
							retryTime = new ExponentialPageLimits(0, 2, 2000);
						}
						long retryNs = 1000000L * retryTime.getPageLimit(retries);

						// +- 30%
						retryNs = retryNs + ThreadLocalRandom.current().nextLong(-retryNs * 30L / 100L, retryNs * 30L / 100L);

						if (retries >= 5 && retries % 5 == 0 || ALWAYS_PRINT_OPTIMISTIC_RETRIES) {
							logger.warn(MARKER_ROCKSDB, "Failed optimistic transaction {} (update):"
									+ " waiting {} ms before retrying for the {} time", LLUtils.toStringSafe(key), retryNs / 1000000d, retries);
						} else if (logger.isDebugEnabled(MARKER_ROCKSDB)) {
							logger.debug(MARKER_ROCKSDB, "Failed optimistic transaction {} (update):"
									+ " waiting {} ms before retrying for the {} time", LLUtils.toStringSafe(key), retryNs / 1000000d, retries);
						}
						// Wait for n milliseconds
						if (retryNs > 0) {
							LockSupport.parkNanos(retryNs);
						}
					}
				} while (!committedSuccessfully);
				if (retries > 5) {
					logger.warn(MARKER_ROCKSDB, "Took {} retries to update key {}", retries, LLUtils.toStringSafe(key));
				}
				recordAtomicUpdateTime(changed, prevData != null, newData != null, initNanoTime);
				optimisticAttempts.record(retries);
				return switch (returnMode) {
					case NOTHING -> RESULT_NOTHING;
					case CURRENT -> new UpdateAtomicResultCurrent(newData);
					case PREVIOUS -> new UpdateAtomicResultPrevious(prevData);
					case BINARY_CHANGED -> new UpdateAtomicResultBinaryChanged(changed);
					case DELTA -> new UpdateAtomicResultDelta(LLDelta.of(prevData, newData));
				};
			}
		} catch (Exception ex) {
			throw new DBException("Failed to update key " + LLUtils.toStringSafe(key), ex);
		}
	}

	@Override
	public boolean supportsTransactions() {
		return true;
	}
}
