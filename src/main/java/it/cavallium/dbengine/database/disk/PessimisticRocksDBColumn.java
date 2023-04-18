package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;

import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.utils.DBException;
import java.io.IOException;
import java.util.concurrent.locks.StampedLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteOptions;

public final class PessimisticRocksDBColumn extends AbstractRocksDBColumn<TransactionDB> {

	private static final TransactionOptions DEFAULT_TX_OPTIONS = new TransactionOptions();

	public PessimisticRocksDBColumn(TransactionDB db,
			String dbName,
			ColumnFamilyHandle cfh,
			MeterRegistry meterRegistry,
			StampedLock closeLock) {
		super(db, dbName, cfh, meterRegistry, closeLock);
	}

	@Override
	protected boolean commitOptimistically(Transaction tx) throws RocksDBException {
		tx.commit();
		return true;
	}

	@Override
	protected Transaction beginTransaction(@NotNull WriteOptions writeOptions,
			TransactionOptions txOpts) {
		return getDb().beginTransaction(writeOptions, txOpts);
	}

	@Override
	public @NotNull UpdateAtomicResult updateAtomicImpl(@NotNull ReadOptions readOptions,
			@NotNull WriteOptions writeOptions,
			Buf key,
			SerializationFunction<@Nullable Buf, @Nullable Buf> updater,
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
				Buf prevData;
				Buf newData;
				boolean changed;
				if (logger.isTraceEnabled()) {
					logger.trace(MARKER_ROCKSDB, "Reading {} (before update lock)", LLUtils.toStringSafe(key));
				}
				var prevDataArray = tx.getForUpdate(readOptions, cfh, keyArray, true);
				try {
					if (logger.isTraceEnabled()) {
						logger.trace(MARKER_ROCKSDB,
								"Reading {}: {} (before update)",
								LLUtils.toStringSafe(key),
								LLUtils.toStringSafe(prevDataArray)
						);
					}
					if (prevDataArray != null) {
						readValueFoundWithoutBloomBufferSize.record(prevDataArray.length);
						prevData = Buf.wrap(prevDataArray);
					} else {
						readValueNotFoundWithoutBloomBufferSize.record(0);
						prevData = null;
					}
					if (prevData != null) {
						prevData.freeze();
					}
					try {
						newData = updater.apply(prevData);
					} catch (Exception ex) {
						throw new DBException("Failed to update key " + LLUtils.toStringSafe(key) + ". The previous value was:\n" + LLUtils.toStringSafe(prevData, 8192), ex);
					}
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
						writeValueBufferSize.record(0);
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
						writeValueBufferSize.record(newDataArray.length);
						tx.put(cfh, keyArray, newDataArray);
						changed = true;
						tx.commit();
					} else {
						changed = false;
						tx.rollback();
					}
				} finally {
					tx.undoGetForUpdate(cfh, keyArray);
				}
				recordAtomicUpdateTime(changed, prevData != null, newData != null, initNanoTime);
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
