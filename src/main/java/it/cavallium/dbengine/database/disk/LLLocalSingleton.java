package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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
		if (Schedulers.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Initialized in a nonblocking thread");
		}
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
	public Mono<byte[]> get(@Nullable LLSnapshot snapshot) {
		return Mono
				.fromCallable(() -> {
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called get in a nonblocking thread");
					}
					return db.get(cfh, resolveSnapshot(snapshot), name);
				})
				.onErrorMap(cause -> new IOException("Failed to read " + Arrays.toString(name), cause));
	}

	@Override
	public Mono<Void> set(byte[] value) {
		return Mono
				.<Void>fromCallable(() -> {
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called set in a nonblocking thread");
					}
					db.put(cfh, name, value);
					return null;
				})
				.onErrorMap(cause -> new IOException("Failed to write " + Arrays.toString(name), cause));
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}
}
