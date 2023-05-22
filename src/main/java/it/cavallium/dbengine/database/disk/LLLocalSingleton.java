package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.disk.UpdateAtomicResultMode.DELTA;

import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.disk.rocksdb.LLReadOptions;
import it.cavallium.dbengine.database.disk.rocksdb.LLWriteOptions;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.utils.DBException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;

public class LLLocalSingleton implements LLSingleton {
	private final RocksDBColumn db;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;
	private final Buf name;
	private final String columnName;
	private final String databaseName;

	public LLLocalSingleton(RocksDBColumn db,
			Function<LLSnapshot, Snapshot> snapshotResolver,
			String databaseName,
			byte[] name,
			String columnName,
			byte @Nullable [] defaultValue) throws RocksDBException {
		this.db = db;
		this.databaseName = databaseName;
		this.snapshotResolver = snapshotResolver;
		this.name = Buf.wrap(name);
		this.columnName = columnName;
		if (LLUtils.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Initialized in a nonblocking thread");
		}
		try (var readOptions = new LLReadOptions()) {
			try (var writeOptions = new LLWriteOptions()) {
				if (defaultValue != null && db.get(readOptions, this.name.asArray(), true) == null) {
					db.put(writeOptions, this.name.asArray(), defaultValue);
				}
			}
		}
	}

	private LLReadOptions generateReadOptions(LLSnapshot snapshot) {
		if (snapshot != null) {
			return new LLReadOptions().setSnapshot(snapshotResolver.apply(snapshot));
		} else {
			return new LLReadOptions();
		}
	}

	@Override
	public Buf get(@Nullable LLSnapshot snapshot) {
		try {
			Buf result;
			try (var readOptions = generateReadOptions(snapshot)) {
				result = db.get(readOptions, name);
			}
			return result;
		} catch (RocksDBException ex) {
			throw new DBException("Failed to read " + LLUtils.toString(name), ex);
		}
	}

	@Override
	public void set(Buf value) {
		try (var writeOptions = new LLWriteOptions()) {
			if (value == null) {
				db.delete(writeOptions, name);
			} else {
				db.put(writeOptions, name, value);
			}
		} catch (RocksDBException ex) {
			throw new DBException("Failed to write " + LLUtils.toString(name), ex);
		}
	}

	private void unset() {
		this.set(null);
	}

	@Override
	public Buf update(SerializationFunction<@Nullable Buf, @Nullable Buf> updater,
			UpdateReturnMode updateReturnMode) {
		if (LLUtils.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called update in a nonblocking thread");
		}
		UpdateAtomicResultMode returnMode = switch (updateReturnMode) {
			case NOTHING -> UpdateAtomicResultMode.NOTHING;
			case GET_NEW_VALUE -> UpdateAtomicResultMode.CURRENT;
			case GET_OLD_VALUE -> UpdateAtomicResultMode.PREVIOUS;
		};
		UpdateAtomicResult result;
		try (var readOptions = new LLReadOptions()) {
			try (var writeOptions = new LLWriteOptions()) {
				result = db.updateAtomic(readOptions, writeOptions, name, updater, returnMode);
			}
		}
		return switch (updateReturnMode) {
			case NOTHING -> null;
			case GET_NEW_VALUE -> ((UpdateAtomicResultCurrent) result).current();
			case GET_OLD_VALUE -> ((UpdateAtomicResultPrevious) result).previous();
		};
	}

	@Override
	public LLDelta updateAndGetDelta(SerializationFunction<@Nullable Buf, @Nullable Buf> updater) {
		if (LLUtils.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called update in a nonblocking thread");
		}
		UpdateAtomicResult result;
		try (var readOptions = new LLReadOptions()) {
			try (var writeOptions = new LLWriteOptions()) {
				result = db.updateAtomic(readOptions, writeOptions, name, updater, DELTA);
			}
		}
		return ((UpdateAtomicResultDelta) result).delta();
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}

	@Override
	public String getColumnName() {
		return columnName;
	}

	@Override
	public String getName() {
		return name.toString(StandardCharsets.UTF_8);
	}
}
