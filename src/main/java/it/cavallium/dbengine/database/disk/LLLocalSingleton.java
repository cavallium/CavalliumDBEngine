package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LLLocalSingleton implements LLSingleton {

	static final ReadOptions EMPTY_READ_OPTIONS = new UnreleasableReadOptions(new UnmodifiableReadOptions());
	static final WriteOptions EMPTY_WRITE_OPTIONS = new UnreleasableWriteOptions(new UnmodifiableWriteOptions());
	private final RocksDBColumn db;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;
	private final byte[] name;
	private final Mono<Send<Buffer>> nameMono;
	private final String databaseName;
	private final Scheduler dbScheduler;

	public LLLocalSingleton(RocksDBColumn db,
			Function<LLSnapshot, Snapshot> snapshotResolver,
			String databaseName,
			byte[] name,
			Scheduler dbScheduler,
			byte[] defaultValue) throws RocksDBException {
		this.db = db;
		this.databaseName = databaseName;
		this.snapshotResolver = snapshotResolver;
		this.name = name;
		this.nameMono = Mono.fromCallable(() -> {
			var alloc = db.getAllocator();
			try (var nameBuf = alloc.allocate(this.name.length)) {
				nameBuf.writeBytes(this.name);
				return nameBuf.send();
			}
		});
		this.dbScheduler = dbScheduler;
		if (Schedulers.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Initialized in a nonblocking thread");
		}
		if (db.get(EMPTY_READ_OPTIONS, this.name, true) == null) {
			db.put(EMPTY_WRITE_OPTIONS, this.name, defaultValue);
		}
	}

	private <T> @NotNull Mono<T> runOnDb(Callable<@Nullable T> callable) {
		return Mono.fromCallable(callable).subscribeOn(dbScheduler);
	}

	private ReadOptions resolveSnapshot(LLSnapshot snapshot) {
		if (snapshot != null) {
			return new ReadOptions().setSnapshot(snapshotResolver.apply(snapshot));
		} else {
			return EMPTY_READ_OPTIONS;
		}
	}

	@Override
	public BufferAllocator getAllocator() {
		return db.getAllocator();
	}

	@Override
	public Mono<byte[]> get(@Nullable LLSnapshot snapshot) {
		return Mono
				.fromCallable(() -> {
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called get in a nonblocking thread");
					}
					return db.get(resolveSnapshot(snapshot), name, true);
				})
				.onErrorMap(cause -> new IOException("Failed to read " + Arrays.toString(name), cause))
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<Void> set(byte[] value) {
		return Mono
				.<Void>fromCallable(() -> {
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called set in a nonblocking thread");
					}
					db.put(EMPTY_WRITE_OPTIONS, name, value);
					return null;
				})
				.onErrorMap(cause -> new IOException("Failed to write " + Arrays.toString(name), cause))
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<Send<Buffer>> update(SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
			UpdateReturnMode updateReturnMode) {
		return Mono.usingWhen(nameMono, keySend -> runOnDb(() -> {
					if (Schedulers.isInNonBlockingThread()) {
						throw new UnsupportedOperationException("Called update in a nonblocking thread");
					}
					UpdateAtomicResultMode returnMode = switch (updateReturnMode) {
						case NOTHING -> UpdateAtomicResultMode.NOTHING;
						case GET_NEW_VALUE -> UpdateAtomicResultMode.CURRENT;
						case GET_OLD_VALUE -> UpdateAtomicResultMode.PREVIOUS;
					};
					UpdateAtomicResult result = db.updateAtomic(EMPTY_READ_OPTIONS, EMPTY_WRITE_OPTIONS, keySend, updater,
							true, returnMode);
					return switch (updateReturnMode) {
						case NOTHING -> null;
						case GET_NEW_VALUE -> ((UpdateAtomicResultCurrent) result).current();
						case GET_OLD_VALUE -> ((UpdateAtomicResultPrevious) result).previous();
					};
				}).onErrorMap(cause -> new IOException("Failed to read or write", cause)),
				keySend -> Mono.fromRunnable(keySend::close));
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}
}
