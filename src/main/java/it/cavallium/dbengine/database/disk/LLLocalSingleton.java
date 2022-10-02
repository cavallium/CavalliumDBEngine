package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.disk.UpdateAtomicResultMode.DELTA;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;
import io.netty5.util.Send;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LLLocalSingleton implements LLSingleton {
	private final RocksDBColumn db;
	private final Function<LLSnapshot, Snapshot> snapshotResolver;
	private final byte[] name;
	private final String columnName;
	private final Mono<Buffer> nameMono;
	private final String databaseName;
	private final Scheduler dbWScheduler;
	private final Scheduler dbRScheduler;

	public LLLocalSingleton(RocksDBColumn db,
			Function<LLSnapshot, Snapshot> snapshotResolver,
			String databaseName,
			byte[] name,
			String columnName,
			Scheduler dbWScheduler,
			Scheduler dbRScheduler,
			byte @Nullable [] defaultValue) throws RocksDBException {
		this.db = db;
		this.databaseName = databaseName;
		this.snapshotResolver = snapshotResolver;
		this.name = name;
		this.columnName = columnName;
		this.nameMono = Mono.fromCallable(() -> {
			var alloc = db.getAllocator();
			var nameBuf = alloc.allocate(this.name.length);
			nameBuf.writeBytes(this.name);
			return nameBuf;
		});
		this.dbWScheduler = dbWScheduler;
		this.dbRScheduler = dbRScheduler;
		if (Schedulers.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Initialized in a nonblocking thread");
		}
		try (var readOptions = new ReadOptions();
				var writeOptions = new WriteOptions()) {
			if (defaultValue != null && db.get(readOptions, this.name, true) == null) {
				db.put(writeOptions, this.name, defaultValue);
			}
		}
	}

	private <T> @NotNull Mono<T> runOnDb(boolean write, Callable<@Nullable T> callable) {
		return Mono.fromCallable(callable).subscribeOn(write ? dbWScheduler : dbRScheduler);
	}

	private ReadOptions generateReadOptions(LLSnapshot snapshot) {
		if (snapshot != null) {
			return new ReadOptions().setSnapshot(snapshotResolver.apply(snapshot));
		} else {
			return new ReadOptions();
		}
	}

	@Override
	public BufferAllocator getAllocator() {
		return db.getAllocator();
	}

	@Override
	public Mono<Buffer> get(@Nullable LLSnapshot snapshot) {
		return nameMono.publishOn(dbRScheduler).handle((name, sink) -> {
			try (name) {
				Buffer result;
				try (var readOptions = generateReadOptions(snapshot)) {
					result = db.get(readOptions, name);
				}
				if (result != null) {
					sink.next(result);
				} else {
					sink.complete();
				}
			} catch (RocksDBException ex) {
				sink.error(new IOException("Failed to read " + LLUtils.toString(name), ex));
			}
		});
	}

	@Override
	public Mono<Void> set(Mono<Buffer> valueMono) {
		return Mono.zip(nameMono, valueMono).publishOn(dbWScheduler).handle((tuple, sink) -> {
			var name = tuple.getT1();
			var value = tuple.getT2();
			try (name; value; var writeOptions = new WriteOptions()) {
				db.put(writeOptions, name, value);
				sink.next(true);
			} catch (RocksDBException ex) {
				sink.error(new IOException("Failed to write " + LLUtils.toString(name), ex));
			}
		}).switchIfEmpty(unset().thenReturn(true)).then();
	}

	private Mono<Void> unset() {
		return nameMono.publishOn(dbWScheduler).handle((name, sink) -> {
			try (name; var writeOptions = new WriteOptions()) {
				db.delete(writeOptions, name);
			} catch (RocksDBException ex) {
				sink.error(new IOException("Failed to read " + LLUtils.toString(name), ex));
			}
		});
	}

	@Override
	public Mono<Buffer> update(BinarySerializationFunction updater,
			UpdateReturnMode updateReturnMode) {
		return Mono.usingWhen(nameMono, key -> runOnDb(true, () -> {
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called update in a nonblocking thread");
			}
			UpdateAtomicResultMode returnMode = switch (updateReturnMode) {
				case NOTHING -> UpdateAtomicResultMode.NOTHING;
				case GET_NEW_VALUE -> UpdateAtomicResultMode.CURRENT;
				case GET_OLD_VALUE -> UpdateAtomicResultMode.PREVIOUS;
			};
			UpdateAtomicResult result;
			try (var readOptions = new ReadOptions(); var writeOptions = new WriteOptions()) {
				result = db.updateAtomic(readOptions, writeOptions, key, updater, returnMode);
			}
			return switch (updateReturnMode) {
				case NOTHING -> {
					result.close();
					yield null;
				}
				case GET_NEW_VALUE -> ((UpdateAtomicResultCurrent) result).current();
				case GET_OLD_VALUE -> ((UpdateAtomicResultPrevious) result).previous();
			};
		}).onErrorMap(cause -> new IOException("Failed to read or write", cause)),
				LLUtils::finalizeResource);
	}

	@Override
	public Mono<LLDelta> updateAndGetDelta(BinarySerializationFunction updater) {
		return Mono.usingWhen(nameMono, key -> runOnDb(true, () -> {
			if (Schedulers.isInNonBlockingThread()) {
				throw new UnsupportedOperationException("Called update in a nonblocking thread");
			}
			UpdateAtomicResult result;
			try (var readOptions = new ReadOptions(); var writeOptions = new WriteOptions()) {
				result = db.updateAtomic(readOptions, writeOptions, key, updater, DELTA);
			}
			return ((UpdateAtomicResultDelta) result).delta();
		}).onErrorMap(cause -> new IOException("Failed to read or write", cause)),
				LLUtils::finalizeResource);
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
		return new String(name);
	}
}
