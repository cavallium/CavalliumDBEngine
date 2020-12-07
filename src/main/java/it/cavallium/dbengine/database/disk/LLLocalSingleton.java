package it.cavallium.dbengine.database.disk;

import java.io.IOException;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;

public class LLLocalSingleton implements LLSingleton {

	private static final ReadOptions EMPTY_READ_OPTIONS = new ReadOptions();
	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;
	private final byte[] name;
	private final String databaseName;

	public LLLocalSingleton(RocksDB db, ColumnFamilyHandle singletonListColumn,
			Function<LLSnapshot, Snapshot> snapshotResolver,
			String databaseName,
			byte[] name,
			byte[] defaultValue) throws RocksDBException {
		this.db = db;
		this.cfh = singletonListColumn;
		this.databaseName = databaseName;
		this.snapshotResolver = snapshotResolver;
		this.name = name;
		if (db.get(cfh, this.name) == null) {
			db.put(cfh, this.name, defaultValue);
		}
	}

	private ReadOptions resolveSnapshot(LLSnapshot snapshot) {
		if (snapshot != null) {
			return new ReadOptions().setSnapshot(snapshotResolver.apply(snapshot));
		} else {
			return EMPTY_READ_OPTIONS;
		}
	}

	@Override
	public byte[] get(@Nullable LLSnapshot snapshot) throws IOException {
		try {
			return db.get(cfh, resolveSnapshot(snapshot), name);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void set(byte[] value) throws IOException {
		try {
			db.put(cfh, name, value);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}
}
