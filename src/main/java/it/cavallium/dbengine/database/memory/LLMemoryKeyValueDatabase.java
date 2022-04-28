package it.cavallium.dbengine.database.memory;

import io.micrometer.core.instrument.MeterRegistry;
import io.netty5.buffer.api.BufferAllocator;
import it.cavallium.dbengine.client.MemoryStats;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.TableWithProperties;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LLMemoryKeyValueDatabase implements LLKeyValueDatabase {

	static {
		LLUtils.initHooks();
	}

	private final BufferAllocator allocator;
	private final MeterRegistry meterRegistry;
	private final String name;
	private final AtomicLong nextSnapshotNumber = new AtomicLong(1);

	private final ConcurrentHashMap<Long, ConcurrentHashMap<String, ConcurrentSkipListMap<ByteList, ByteList>>> snapshots = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, ConcurrentSkipListMap<ByteList, ByteList>> mainDb;
	private final ConcurrentHashMap<String, LLMemoryDictionary> singletons = new ConcurrentHashMap<>();

	public LLMemoryKeyValueDatabase(BufferAllocator allocator,
			MeterRegistry meterRegistry,
			String name,
			List<Column> columns) {
		this.allocator = allocator;
		this.meterRegistry = meterRegistry;
		this.name = name;
		this.mainDb = new ConcurrentHashMap<>();
		for (Column column : columns) {
			mainDb.put(column.name(), new ConcurrentSkipListMap<>());
		}
		this.snapshots.put(Long.MIN_VALUE + 1L, this.mainDb);
	}

	@Override
	public Mono<? extends LLSingleton> getSingleton(byte[] singletonListColumnName,
			byte[] singletonName,
			byte @Nullable[] defaultValue) {
		var columnNameString = new String(singletonListColumnName, StandardCharsets.UTF_8);
		var dict = singletons.computeIfAbsent(columnNameString, _unused -> new LLMemoryDictionary(allocator,
				name,
				columnNameString,
				UpdateMode.ALLOW,
				snapshots,
				mainDb
		));
		return Mono
				.fromCallable(() -> new LLMemorySingleton(dict, columnNameString, singletonName)).flatMap(singleton -> singleton
						.get(null)
						.transform(mono -> {
							if (defaultValue != null) {
								return mono.switchIfEmpty(singleton
										.set(Mono.fromSupplier(() -> allocator.copyOf(defaultValue).send()))
										.then(Mono.empty()));
							} else {
								return mono;
							}
						})
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
	public Mono<MemoryStats> getMemoryStats() {
		return Mono.just(new MemoryStats(0, 0, 0, 0, 0, 0));
	}

	@Override
	public Mono<String> getRocksDBStats() {
		return Mono.empty();
	}

	@Override
	public Flux<TableWithProperties> getTableProperties() {
		return Flux.empty();
	}

	@Override
	public Mono<Void> verifyChecksum() {
		return Mono.empty();
	}

	@Override
	public Mono<Void> compact() {
		return Mono.empty();
	}

	@Override
	public Mono<Void> flush() {
		return Mono.empty();
	}

	@Override
	public BufferAllocator getAllocator() {
		return allocator;
	}

	@Override
	public MeterRegistry getMeterRegistry() {
		return meterRegistry;
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
