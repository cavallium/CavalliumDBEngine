package it.cavallium.dbengine.database.memory;

import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.MemoryStats;
import it.cavallium.dbengine.database.ColumnProperty;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.RocksDBLongProperty;
import it.cavallium.dbengine.database.RocksDBMapProperty;
import it.cavallium.dbengine.database.RocksDBStringProperty;
import it.cavallium.dbengine.database.TableWithProperties;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.rpc.current.data.Column;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

public class LLMemoryKeyValueDatabase implements LLKeyValueDatabase {
	private final MeterRegistry meterRegistry;
	private final String name;
	private final AtomicLong nextSnapshotNumber = new AtomicLong(1);

	private final ConcurrentHashMap<Long, ConcurrentHashMap<String, ConcurrentSkipListMap<Buf, Buf>>> snapshots = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, ConcurrentSkipListMap<Buf, Buf>> mainDb;
	private final ConcurrentHashMap<String, LLMemoryDictionary> singletons = new ConcurrentHashMap<>();

	public LLMemoryKeyValueDatabase(MeterRegistry meterRegistry,
			String name,
			List<Column> columns) {
		this.meterRegistry = meterRegistry;
		this.name = name;
		this.mainDb = new ConcurrentHashMap<>();
		for (Column column : columns) {
			mainDb.put(column.name(), new ConcurrentSkipListMap<>());
		}
		this.snapshots.put(Long.MIN_VALUE + 1L, this.mainDb);
	}

	@Override
	public LLSingleton getSingleton(byte[] singletonListColumnName,
			byte[] singletonName,
			byte @Nullable[] defaultValue) {
		var columnNameString = new String(singletonListColumnName, StandardCharsets.UTF_8);
		var dict = singletons.computeIfAbsent(columnNameString, _unused -> new LLMemoryDictionary(name,
				columnNameString,
				UpdateMode.ALLOW,
				snapshots,
				mainDb
		));
		var singleton = new LLMemorySingleton(dict, columnNameString, singletonName);
		Buf returnValue = singleton.get(null);
		if (returnValue == null && defaultValue != null) {
			singleton.set(Buf.wrap(defaultValue));
		}
		return singleton;
	}

	@Override
	public LLDictionary getDictionary(byte[] columnName, UpdateMode updateMode) {
		var columnNameString = new String(columnName, StandardCharsets.UTF_8);
		return new LLMemoryDictionary(name,
				columnNameString,
				updateMode,
				snapshots,
				mainDb
		);
	}

	@Override
	public MemoryStats getMemoryStats() {
		return new MemoryStats(0, 0, 0, 0, 0, 0);
	}

	@Override
	public String getRocksDBStats() {
		return null;
	}

	@Override
	public Map<String, String> getMapProperty(@Nullable Column column, RocksDBMapProperty property) {
		return null;
	}

	@Override
	public Stream<ColumnProperty<Map<String, String>>> getMapColumnProperties(RocksDBMapProperty property) {
		return Stream.empty();
	}

	@Override
	public String getStringProperty(@Nullable Column column, RocksDBStringProperty property) {
		return null;
	}

	@Override
	public Stream<ColumnProperty<String>> getStringColumnProperties(RocksDBStringProperty property) {
		return Stream.empty();
	}

	@Override
	public Long getLongProperty(@Nullable Column column, RocksDBLongProperty property) {
		return null;
	}

	@Override
	public Stream<ColumnProperty<Long>> getLongColumnProperties(RocksDBLongProperty property) {
		return Stream.empty();
	}

	@Override
	public Long getAggregatedLongProperty(RocksDBLongProperty property) {
		return null;
	}

	@Override
	public Stream<TableWithProperties> getTableProperties() {
		return Stream.empty();
	}

	@Override
	public void verifyChecksum() {
	}

	@Override
	public void compact() {
	}

	@Override
	public void flush() {
	}

	@Override
	public MeterRegistry getMeterRegistry() {
		return meterRegistry;
	}

	@Override
	public void preClose() {
	}

	@Override
	public void close() {
		snapshots.forEach((snapshot, dbs) -> dbs.forEach((columnName, db) -> {
			db.clear();
		}));
		mainDb.forEach((columnName, db) -> {
			db.clear();
		});
	}

	@Override
	public String getDatabaseName() {
		return name;
	}

	@Override
	public LLSnapshot takeSnapshot() {
		var snapshotNumber = nextSnapshotNumber.getAndIncrement();
		var snapshot = new ConcurrentHashMap<String, ConcurrentSkipListMap<Buf, Buf>>();
		mainDb.forEach((columnName, column) -> {
			var cloned = column.clone();
			snapshot.put(columnName, cloned);
		});
		snapshots.put(snapshotNumber, snapshot);
		return new LLSnapshot(snapshotNumber);
	}

	@Override
	public void releaseSnapshot(LLSnapshot snapshot) {
		snapshots.remove(snapshot.getSequenceNumber());
	}

	@Override
	public void pauseForBackup() {
	}

	@Override
	public void resumeAfterBackup() {
	}

	@Override
	public boolean isPaused() {
		return false;
	}

	@Override
	public void ingestSST(Column column, Stream<Path> files, boolean replaceExisting) {
		throw new UnsupportedOperationException("Memory db doesn't support SST files");
	}
}
