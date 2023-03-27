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
import org.rocksdb.RocksDB;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteOptions;

public final class StandardRocksDBColumn extends AbstractRocksDBColumn<RocksDB> {

	public StandardRocksDBColumn(RocksDB db,
			String dbName,
			ColumnFamilyHandle cfh,
			MeterRegistry meterRegistry,
			StampedLock closeLock) {
		super(db, dbName, cfh, meterRegistry, closeLock);
	}

	@Override
	protected boolean commitOptimistically(Transaction tx) {
		throw new UnsupportedOperationException("Transactions not supported");
	}

	@Override
	protected Transaction beginTransaction(@NotNull WriteOptions writeOptions,
			TransactionOptions txOpts) {
		throw new UnsupportedOperationException("Transactions not supported");
	}

	@Override
	public @NotNull UpdateAtomicResult updateAtomicImpl(@NotNull ReadOptions readOptions,
			@NotNull WriteOptions writeOptions,
			Buf key,
			SerializationFunction<@Nullable Buf, @Nullable Buf> updater,
			UpdateAtomicResultMode returnMode) {
		long initNanoTime = System.nanoTime();
		try {
			@Nullable Buf prevData = this.get(readOptions, key);
			if (logger.isTraceEnabled()) {
				logger.trace(MARKER_ROCKSDB,
						"Reading {}: {} (before update)",
						LLUtils.toStringSafe(key),
						LLUtils.toStringSafe(prevData)
				);
			}

			Buf prevDataToSendToUpdater;
			if (prevData != null) {
				prevDataToSendToUpdater = prevData.copy();
			} else {
				prevDataToSendToUpdater = null;
			}

			@Nullable Buf newData;
			newData = updater.apply(prevDataToSendToUpdater);
			boolean changed;
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
				Buf dataToPut;
				if (returnMode == UpdateAtomicResultMode.CURRENT) {
					dataToPut = newData.copy();
				} else {
					dataToPut = newData;
				}
				this.put(writeOptions, key, dataToPut);
				changed = true;
			} else {
				changed = false;
			}
			recordAtomicUpdateTime(changed, prevData != null, newData != null, initNanoTime);
			return switch (returnMode) {
				case NOTHING -> RESULT_NOTHING;
				case CURRENT -> new UpdateAtomicResultCurrent(newData != null ? newData.copy() : null);
				case PREVIOUS -> new UpdateAtomicResultPrevious(prevData != null ? prevData.copy() : null);
				case BINARY_CHANGED -> new UpdateAtomicResultBinaryChanged(changed);
				case DELTA -> new UpdateAtomicResultDelta(LLDelta.of(
						prevData != null ? prevData.copy() : null,
						newData != null ? newData.copy() : null));
			};
		} catch (Exception ex) {
			throw new DBException("Failed to update key " + LLUtils.toStringSafe(key), ex);
		}
	}

	@Override
	public boolean supportsTransactions() {
		return false;
	}
}
