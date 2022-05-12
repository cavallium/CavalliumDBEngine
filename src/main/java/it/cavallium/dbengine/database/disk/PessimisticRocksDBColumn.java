package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;

import io.micrometer.core.instrument.MeterRegistry;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.MemoryManager;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.rocksdb.RocksObj;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
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
import reactor.core.scheduler.Schedulers;

public final class PessimisticRocksDBColumn extends AbstractRocksDBColumn<TransactionDB> {

	private static final TransactionOptions DEFAULT_TX_OPTIONS = new TransactionOptions();

	public PessimisticRocksDBColumn(TransactionDB db,
			boolean nettyDirect,
			BufferAllocator alloc,
			String dbName,
			RocksObj<ColumnFamilyHandle> cfh,
			MeterRegistry meterRegistry,
			StampedLock closeLock) {
		super(db, nettyDirect, alloc, dbName, cfh, meterRegistry, closeLock);
	}

	@Override
	protected boolean commitOptimistically(RocksObj<Transaction> tx) throws RocksDBException {
		tx.v().commit();
		return true;
	}

	@Override
	protected RocksObj<Transaction> beginTransaction(@NotNull RocksObj<WriteOptions> writeOptions,
			RocksObj<TransactionOptions> txOpts) {
		return new RocksObj<>(getDb().beginTransaction(writeOptions.v(), txOpts.v()));
	}

	@Override
	public @NotNull UpdateAtomicResult updateAtomicImpl(@NotNull RocksObj<ReadOptions> readOptions,
			@NotNull RocksObj<WriteOptions> writeOptions,
			Buffer key,
			BinarySerializationFunction updater,
			UpdateAtomicResultMode returnMode) throws IOException {
		long initNanoTime = System.nanoTime();
		try {
			var cfh = getCfh();
			var keyArray = LLUtils.toArray(key);
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called update in a nonblocking thread");
			}
			try (var txOpts = new RocksObj<>(new TransactionOptions());
					var tx = beginTransaction(writeOptions, txOpts)) {
				Send<Buffer> sentPrevData;
				Send<Buffer> sentCurData;
				boolean changed;
				if (logger.isTraceEnabled()) {
					logger.trace(MARKER_ROCKSDB, "Reading {} (before update lock)", LLUtils.toStringSafe(key));
				}
				var prevDataArray = tx.v().getForUpdate(readOptions.v(), cfh.v(), keyArray, true);
				try {
					if (logger.isTraceEnabled()) {
						logger.trace(MARKER_ROCKSDB,
								"Reading {}: {} (before update)",
								LLUtils.toStringSafe(key),
								LLUtils.toStringSafe(prevDataArray)
						);
					}
					Buffer prevData;
					if (prevDataArray != null) {
						readValueFoundWithoutBloomBufferSize.record(prevDataArray.length);
						prevData = MemoryManager.unsafeWrap(prevDataArray);
					} else {
						readValueNotFoundWithoutBloomBufferSize.record(0);
						prevData = null;
					}
					try (prevData) {
						Buffer prevDataToSendToUpdater;
						if (prevData != null) {
							prevDataToSendToUpdater = prevData.copy().makeReadOnly();
						} else {
							prevDataToSendToUpdater = null;
						}

						@Nullable Buffer newData = applyUpdateAndCloseIfNecessary(updater, prevDataToSendToUpdater);
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
								writeValueBufferSize.record(0);
								tx.v().delete(cfh.v(), keyArray, true);
								changed = true;
								tx.v().commit();
							} else if (newData != null && (prevData == null || !LLUtils.equals(prevData, newData))) {
								if (logger.isTraceEnabled()) {
									logger.trace(MARKER_ROCKSDB,
											"Writing {}: {} (after update)",
											LLUtils.toStringSafe(key),
											LLUtils.toStringSafe(newData)
									);
								}
								writeValueBufferSize.record(newDataArray.length);
								tx.v().put(cfh.v(), keyArray, newDataArray);
								changed = true;
								tx.v().commit();
							} else {
								changed = false;
								tx.v().rollback();
							}
							sentPrevData = prevData == null ? null : prevData.send();
							sentCurData = newData == null ? null : newData.send();
						}
					}
				} finally {
					tx.v().undoGetForUpdate(cfh.v(), keyArray);
				}
				recordAtomicUpdateTime(changed, sentPrevData != null, sentCurData != null, initNanoTime);
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
		} catch (Throwable ex) {
			throw new IOException("Failed to update key " + LLUtils.toStringSafe(key), ex);
		}
	}

	@Override
	public boolean supportsTransactions() {
		return true;
	}
}
