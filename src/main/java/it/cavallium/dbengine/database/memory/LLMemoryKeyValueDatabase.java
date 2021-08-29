package it.cavallium.dbengine.database.memory;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSnapshottable;
import it.cavallium.dbengine.database.UpdateMode;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LLMemoryKeyValueDatabase implements LLKeyValueDatabase {

	private final ByteBufAllocator allocator;
	private final String name;
	private final AtomicLong nextSnapshotNumber = new AtomicLong(1);

	private final ConcurrentHashMap<Long, ConcurrentHashMap<String, ConcurrentSkipListMap<ByteList, ByteList>>> snapshots = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, ConcurrentSkipListMap<ByteList, ByteList>> mainDb;
	private final ConcurrentHashMap<String, LLMemoryDictionary> singletons = new ConcurrentHashMap<>();

	public LLMemoryKeyValueDatabase(ByteBufAllocator allocator, String name, List<Column> columns) {
		this.allocator = allocator;
		this.name = name;
		this.mainDb = new ConcurrentHashMap<>();
		for (Column column : columns) {
			mainDb.put(column.name(), new ConcurrentSkipListMap<>());
		}
		this.snapshots.put(Long.MIN_VALUE + 1L, this.mainDb);
	}

	@Override
	public Mono<? extends LLSingleton> getSingleton(byte[] singletonListColumnName, byte[] singletonName, byte[] defaultValue) {
		var columnNameString = new String(singletonListColumnName, StandardCharsets.UTF_8);
		var dict = singletons.computeIfAbsent(columnNameString, _unused -> new LLMemoryDictionary(allocator,
				name,
				columnNameString,
				UpdateMode.ALLOW,
				snapshots,
				mainDb
		));
		return Mono
				.fromCallable(() -> new LLMemorySingleton(dict, singletonName)).flatMap(singleton -> singleton
						.get(null)
						.switchIfEmpty(singleton.set(defaultValue).then(Mono.empty()))
						.thenReturn(singleton)
				);
	}

	@Override
	public Mono<? extends LLDictionary> getDictionary(byte[] columnName, UpdateMode updateMode) {
		var columnNameString = new String(columnName, StandardCharsets.UTF_8);
		return Mono.fromCallable(() -> new LLMemoryDictionary(allocator,
				name,
				columnNameString,
				updateMode,
				snapshots,
				mainDb
		));
	}

	@Override
	public Mono<Long> getProperty(String propertyName) {
		return Mono.empty();
	}

	@Override
	public Mono<Void> verifyChecksum() {
		return Mono.empty();
	}

	@Override
	public ByteBufAllocator getAllocator() {
		return allocator;
	}

	@Override
	public Mono<Void> close() {
		return Mono
				.fromRunnable(() -> {
					snapshots.forEach((snapshot, dbs) -> dbs.forEach((columnName, db) -> {
						db.clear();
					}));
					mainDb.forEach((columnName, db) -> {
						db.clear();
					});
				});
	}

	@Override
	public String getDatabaseName() {
		return name;
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return Mono
				.fromCallable(() -> {
					var snapshotNumber = nextSnapshotNumber.getAndIncrement();
					var snapshot = new ConcurrentHashMap<String, ConcurrentSkipListMap<ByteList, ByteList>>();
					mainDb.forEach((columnName, column) -> {
						var cloned = column.clone();
						snapshot.put(columnName, cloned);
					});
					snapshots.put(snapshotNumber, snapshot);
					return new LLSnapshot(snapshotNumber);
				});
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono
				.fromCallable(() -> snapshots.remove(snapshot.getSequenceNumber()))
				.then();
	}
}
