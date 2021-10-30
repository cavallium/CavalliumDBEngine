package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;

import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Holder;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

public final class StandardRocksDBColumn extends AbstractRocksDBColumn<RocksDB> {

	public StandardRocksDBColumn(RocksDB db,
			DatabaseOptions databaseOptions,
			BufferAllocator alloc,
			ColumnFamilyHandle cfh, MeterRegistry meterRegistry) {
		super(db, databaseOptions, alloc, cfh, meterRegistry);
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
	public @NotNull UpdateAtomicResult updateAtomic(@NotNull ReadOptions readOptions,
			@NotNull WriteOptions writeOptions,
			Send<Buffer> keySend,
			SerializationFunction<@Nullable Send<Buffer>, @Nullable Send<Buffer>> updater,
			boolean existsAlmostCertainly,
			UpdateAtomicResultMode returnMode) throws IOException, RocksDBException {
		try (Buffer key = keySend.receive()) {
			var cfh = getCfh();
			var db = getDb();
			var alloc = getAllocator();
			@Nullable Buffer prevData;
			var prevDataHolder = existsAlmostCertainly ? null : new Holder<byte[]>();
			if (existsAlmostCertainly || db.keyMayExist(cfh, LLUtils.toArray(key), prevDataHolder)) {
				if (!existsAlmostCertainly && prevDataHolder.getValue() != null) {
					byte @Nullable [] prevDataBytes = prevDataHolder.getValue();
					if (prevDataBytes != null) {
						prevData = LLUtils.fromByteArray(alloc, prevDataBytes);
					} else {
						prevData = null;
					}
				} else {
					var obtainedPrevData = this.get(readOptions, key.copy().send(), existsAlmostCertainly);
					if (obtainedPrevData == null) {
						prevData = null;
					} else {
						prevData = obtainedPrevData.receive();
					}
				}
			} else {
				prevData = null;
			}
			if (logger.isTraceEnabled()) {
				logger.trace(MARKER_ROCKSDB, "Reading {}: {} (before update)", LLUtils.toStringSafe(key),
						LLUtils.toStringSafe(prevData));
			}
			try {
				@Nullable Buffer newData;
				try (Buffer prevDataToSendToUpdater = prevData == null ? null : prevData.copy()) {
					try (var sentData = prevDataToSendToUpdater == null ? null : prevDataToSendToUpdater.send()) {
						try (var newDataToReceive = updater.apply(sentData)) {
							if (newDataToReceive != null) {
								newData = newDataToReceive.receive();
							} else {
								newData = null;
							}
						}
					}
				}
				boolean changed;
				assert newData == null || newData.isAccessible();
				try {
					if (logger.isTraceEnabled()) {
						logger.trace(MARKER_ROCKSDB,
								"Updating {}. previous data: {}, updated data: {}", LLUtils.toStringSafe(key),
								LLUtils.toStringSafe(prevData), LLUtils.toStringSafe(newData));
					}
					if (prevData != null && newData == null) {
						if (logger.isTraceEnabled()) {
							logger.trace(MARKER_ROCKSDB, "Deleting {} (after update)", LLUtils.toStringSafe(key));
						}
						this.delete(writeOptions, key.send());
						changed = true;
					} else if (newData != null && (prevData == null || !LLUtils.equals(prevData, newData))) {
						if (logger.isTraceEnabled()) {
							logger.trace(MARKER_ROCKSDB, "Writing {}: {} (after update)", LLUtils.toStringSafe(key),
									LLUtils.toStringSafe(newData));
						}
						Buffer dataToPut;
						if (returnMode == UpdateAtomicResultMode.CURRENT) {
							dataToPut = newData.copy();
						} else {
							dataToPut = newData;
						}
						try {
							this.put(writeOptions, key.send(), dataToPut.send());
							changed = true;
						} finally {
							if (dataToPut != newData) {
								dataToPut.close();
							}
						}
					} else {
						changed = false;
					}
					return switch (returnMode) {
						case NOTHING -> {
							if (prevData != null) {
								prevData.close();
							}
							if (newData != null) {
								newData.close();
							}
							yield RESULT_NOTHING;
						}
						case CURRENT -> {
							if (prevData != null) {
								prevData.close();
							}
							yield new UpdateAtomicResultCurrent(newData != null ? newData.send() : null);
						}
						case PREVIOUS -> {
							if (newData != null) {
								newData.close();
							}
							yield new UpdateAtomicResultPrevious(prevData != null ? prevData.send() : null);
						}
						case BINARY_CHANGED -> new UpdateAtomicResultBinaryChanged(changed);
						case DELTA -> new UpdateAtomicResultDelta(LLDelta
								.of(prevData != null ? prevData.send() : null, newData != null ? newData.send() : null)
								.send());
					};
				} finally {
					if (newData != null) {
						newData.close();
					}
				}
			} finally {
				if (prevData != null) {
					prevData.close();
				}
			}
		}
	}

	@Override
	public boolean supportsTransactions() {
		return false;
	}
}
