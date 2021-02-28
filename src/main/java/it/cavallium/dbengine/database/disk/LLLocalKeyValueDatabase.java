package it.cavallium.dbengine.database.disk;

import com.google.common.base.Suppliers;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.UpdateMode;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DbPath;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WALRecoveryMode;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LLLocalKeyValueDatabase implements LLKeyValueDatabase {

	static {
		RocksDB.loadLibrary();
	}

	private static final ColumnFamilyDescriptor DEFAULT_COLUMN_FAMILY = new ColumnFamilyDescriptor(
			RocksDB.DEFAULT_COLUMN_FAMILY);
	private static final Supplier<Scheduler> lowMemorySupplier = Suppliers.memoize(() ->
			Schedulers.newSingle("db-low-memory"))::get;

	private final Scheduler dbScheduler;
	private final Path dbPath;
	private final String name;
	private RocksDB db;
	private final Map<Column, ColumnFamilyHandle> handles;
	private final ConcurrentHashMap<Long, Snapshot> snapshotsHandles = new ConcurrentHashMap<>();
	private final AtomicLong nextSnapshotNumbers = new AtomicLong(1);

	public LLLocalKeyValueDatabase(String name, Path path, List<Column> columns, List<ColumnFamilyHandle> handles,
			boolean crashIfWalError, boolean lowMemory) throws IOException {
		Options options = openRocksDb(path, crashIfWalError, lowMemory);
		try {
			List<ColumnFamilyDescriptor> descriptors = new LinkedList<>();
			descriptors
					.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
			for (Column column : columns) {
				descriptors
						.add(new ColumnFamilyDescriptor(column.getName().getBytes(StandardCharsets.US_ASCII)));
			}

			// Get databases directory path
			Path databasesDirPath = path.toAbsolutePath().getParent();

			String dbPathString = databasesDirPath.toString() + File.separatorChar + path.getFileName();
			Path dbPath = Paths.get(dbPathString);
			this.dbPath = dbPath;
			this.name = name;
			if (lowMemory) {
				this.dbScheduler = lowMemorySupplier.get();
			} else {
				this.dbScheduler = Schedulers.newBoundedElastic(Runtime.getRuntime().availableProcessors(),
						Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
						"db-" + name,
						60,
						true
				);
			}

			createIfNotExists(descriptors, options, dbPath, dbPathString);
			// Create all column families that don't exist
			createAllColumns(descriptors, options, dbPathString);

			// a factory method that returns a RocksDB instance
			this.db = RocksDB.open(new DBOptions(options), dbPathString, descriptors, handles);
			this.handles = new HashMap<>();
			for (int i = 0; i < columns.size(); i++) {
				this.handles.put(columns.get(i), handles.get(i));
			}

			flushDb(db, handles);
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public String getDatabaseName() {
		return name;
	}

	private void flushAndCloseDb(RocksDB db, List<ColumnFamilyHandle> handles)
			throws RocksDBException {
		flushDb(db, handles);

		for (ColumnFamilyHandle handle : handles) {
			handle.close();
		}

		db.closeE();
	}

	private void flushDb(RocksDB db, List<ColumnFamilyHandle> handles) throws RocksDBException {
		// force flush the database
		for (int i = 0; i < 2; i++) {
			db.flush(new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true), handles);
			db.flushWal(true);
			db.syncWal();
		}
		// end force flush
	}

	@SuppressWarnings("CommentedOutCode")
	private static Options openRocksDb(Path path, boolean crashIfWalError, boolean lowMemory)
			throws IOException {
		// Get databases directory path
		Path databasesDirPath = path.toAbsolutePath().getParent();
		// Create base directories
		if (Files.notExists(databasesDirPath)) {
			Files.createDirectories(databasesDirPath);
		}

		// the Options class contains a set of configurable DB options
		// that determines the behaviour of the database.
		var options = new Options();
		options.setCreateIfMissing(true);
		options.setCompactionStyle(CompactionStyle.LEVEL);
		options.setLevelCompactionDynamicLevelBytes(true);
		options.setTargetFileSizeBase(64 * 1024 * 1024); // 64MiB sst file
		options.setMaxBytesForLevelBase(4 * 256 * 1024 * 1024); // 4 times the sst file
		options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
		options.setManualWalFlush(false);
		options.setMinWriteBufferNumberToMerge(3);
		options.setMaxWriteBufferNumber(4);
		options.setWalTtlSeconds(30); // flush wal after 30 seconds
		options.setAvoidFlushDuringShutdown(false); // Flush all WALs during shutdown
		options.setAvoidFlushDuringRecovery(false); // Flush all WALs during startup
		options.setWalRecoveryMode(crashIfWalError
				? WALRecoveryMode.AbsoluteConsistency
				: WALRecoveryMode.PointInTimeRecovery); // Crash if the WALs are corrupted.Default: TolerateCorruptedTailRecords
		options.setDeleteObsoleteFilesPeriodMicros(20 * 1000000); // 20 seconds
		options.setPreserveDeletes(false);
		options.setKeepLogFileNum(10);
		// Direct I/O parameters. Removed because they use too much disk.
		//options.setUseDirectReads(true);
		//options.setUseDirectIoForFlushAndCompaction(true);
		//options.setCompactionReadaheadSize(2 * 1024 * 1024); // recommend at least 2MB
		//options.setWritableFileMaxBufferSize(1024 * 1024); // 1MB by default
		if (lowMemory) {
			// LOW MEMORY
			options
					.setBytesPerSync(1024 * 1024)
					.setWalBytesPerSync(1024 * 1024)
					.setIncreaseParallelism(1)
					.setMaxOpenFiles(2)
					.optimizeLevelStyleCompaction(1024 * 1024) // 1MiB of ram will be used for level style compaction
					.setWriteBufferSize(1024 * 1024) // 1MB
					.setWalSizeLimitMB(16) // 16MB
					.setMaxTotalWalSize(1024L * 1024L * 1024L) // 1GiB max wal directory size
					.setDbPaths(List.of(new DbPath(databasesDirPath.resolve(path.getFileName() + "_hot"),
									400L * 1024L * 1024L * 1024L), // 400GiB
							new DbPath(databasesDirPath.resolve(path.getFileName() + "_cold"),
									600L * 1024L * 1024L * 1024L))) // 600GiB
			;
		} else {
			// HIGH MEMORY
			options
					.setAllowConcurrentMemtableWrite(true)
					.setEnableWriteThreadAdaptiveYield(true)
					.setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
					.setBytesPerSync(10 * 1024 * 1024)
					.setWalBytesPerSync(10 * 1024 * 1024)
					.optimizeLevelStyleCompaction(
							128 * 1024 * 1024) // 128MiB of ram will be used for level style compaction
					.setWriteBufferSize(128 * 1024 * 1024) // 128MB
					.setWalSizeLimitMB(1024) // 1024MB
					.setMaxTotalWalSize(8L * 1024L * 1024L * 1024L) // 8GiB max wal directory size
					.setDbPaths(List.of(new DbPath(databasesDirPath.resolve(path.getFileName() + "_hot"),
									400L * 1024L * 1024L * 1024L), // 400GiB
							new DbPath(databasesDirPath.resolve(path.getFileName() + "_cold"),
									600L * 1024L * 1024L * 1024L))) // 600GiB
			;
		}

		final BloomFilter bloomFilter = new BloomFilter(10, false);
		final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
		tableOptions.setFilterPolicy(bloomFilter);
		options.setTableFormatConfig(tableOptions);

		return options;
	}

	private void createAllColumns(List<ColumnFamilyDescriptor> totalDescriptors, Options options,
			String dbPathString) throws RocksDBException {
		List<byte[]> columnFamiliesToCreate = new LinkedList<>();

		for (ColumnFamilyDescriptor descriptor : totalDescriptors) {
			columnFamiliesToCreate.add(descriptor.getName());
		}

		List<byte[]> existingColumnFamilies = RocksDB.listColumnFamilies(options, dbPathString);

		columnFamiliesToCreate.removeIf((columnFamilyName) -> {
			for (byte[] cfn : existingColumnFamilies) {
				if (Arrays.equals(cfn, columnFamilyName)) {
					return true;
				}
			}
			return false;
		});

		List<ColumnFamilyDescriptor> descriptors = new LinkedList<>();
		descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
		for (byte[] existingColumnFamily : existingColumnFamilies) {
			descriptors.add(new ColumnFamilyDescriptor(existingColumnFamily));
		}

		var handles = new LinkedList<ColumnFamilyHandle>();

		/*
		  SkipStatsUpdateOnDbOpen = true because this RocksDB.open session is used only to add just some columns
		 */
		//var dbOptionsFastLoadSlowEdit = new DBOptions(options.setSkipStatsUpdateOnDbOpen(true));

		this.db = RocksDB.open(new DBOptions(options), dbPathString, descriptors, handles);

		for (byte[] name : columnFamiliesToCreate) {
			db.createColumnFamily(new ColumnFamilyDescriptor(name)).close();
		}

		flushAndCloseDb(db, handles);
	}

	private void createIfNotExists(List<ColumnFamilyDescriptor> descriptors, Options options,
			Path dbPath, String dbPathString) throws RocksDBException {
		if (Files.notExists(dbPath)) {
			// Check if handles are all different
			var descriptorsSet = new HashSet<>(descriptors);
			if (descriptorsSet.size() != descriptors.size()) {
				throw new IllegalArgumentException("Descriptors must be unique!");
			}

			List<ColumnFamilyDescriptor> descriptorsToCreate = new LinkedList<>(descriptors);
			descriptorsToCreate
					.removeIf((cf) -> Arrays.equals(cf.getName(), DEFAULT_COLUMN_FAMILY.getName()));

			/*
			  SkipStatsUpdateOnDbOpen = true because this RocksDB.open session is used only to add just some columns
			 */
			//var dbOptionsFastLoadSlowEdit = options.setSkipStatsUpdateOnDbOpen(true);

			LinkedList<ColumnFamilyHandle> handles = new LinkedList<>();

			this.db = RocksDB.open(options, dbPathString);
			for (ColumnFamilyDescriptor columnFamilyDescriptor : descriptorsToCreate) {
				handles.add(db.createColumnFamily(columnFamilyDescriptor));
			}

			flushAndCloseDb(db, handles);
		}
	}

	@Override
	public Mono<LLLocalSingleton> getSingleton(byte[] singletonListColumnName, byte[] name, byte[] defaultValue) {
		return Mono
				.fromCallable(() -> new LLLocalSingleton(db,
						handles.get(Column.special(Column.toString(singletonListColumnName))),
						(snapshot) -> snapshotsHandles.get(snapshot.getSequenceNumber()),
						LLLocalKeyValueDatabase.this.name,
						name,
						dbScheduler,
						defaultValue
				))
				.onErrorMap(IOException::new)
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<LLLocalDictionary> getDictionary(byte[] columnName, UpdateMode updateMode) {
		return Mono
				.fromCallable(() -> new LLLocalDictionary(db,
						handles.get(Column.special(Column.toString(columnName))),
						name,
						dbScheduler,
						(snapshot) -> snapshotsHandles.get(snapshot.getSequenceNumber()),
						updateMode
				))
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<Long> getProperty(String propertyName) {
		return Mono.fromCallable(() -> db.getAggregatedLongProperty(propertyName))
				.onErrorMap(IOException::new)
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return Mono
				.fromCallable(() -> {
					var snapshot = db.getSnapshot();
					long currentSnapshotSequenceNumber = nextSnapshotNumbers.getAndIncrement();
					this.snapshotsHandles.put(currentSnapshotSequenceNumber, snapshot);
					return new LLSnapshot(currentSnapshotSequenceNumber);
				})
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono
				.<Void>fromCallable(() -> {
					Snapshot dbSnapshot = this.snapshotsHandles.remove(snapshot.getSequenceNumber());
					if (dbSnapshot == null) {
						throw new IOException("Snapshot " + snapshot.getSequenceNumber() + " not found!");
					}
					db.releaseSnapshot(dbSnapshot);
					return null;
				})
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<Void> close() {
		return Mono
				.<Void>fromCallable(() -> {
					try {
						flushAndCloseDb(db, new ArrayList<>(handles.values()));
						deleteUnusedOldLogFiles();
					} catch (RocksDBException e) {
						throw new IOException(e);
					}
					return null;
				})
				.onErrorMap(IOException::new)
				.subscribeOn(dbScheduler);
	}

	/**
	 * Call this method ONLY AFTER flushing completely a db and closing it!
	 */
	@SuppressWarnings("unused")
	private void deleteUnusedOldLogFiles() {
		Path basePath = dbPath;
		try {
			Files
					.walk(basePath, 1)
					.filter(p -> !p.equals(basePath))
					.filter(p -> {
						var fileName = p.getFileName().toString();
						if (fileName.startsWith("LOG.old.")) {
							var parts = fileName.split("\\.");
							if (parts.length == 3) {
								try {
									long nameSuffix = Long.parseUnsignedLong(parts[2]);
									return true;
								} catch (NumberFormatException ex) {
									return false;
								}
							}
						}
						if (fileName.endsWith(".log")) {
							var parts = fileName.split("\\.");
							if (parts.length == 2) {
								try {
									int name = Integer.parseUnsignedInt(parts[0]);
									return true;
								} catch (NumberFormatException ex) {
									return false;
								}
							}
						}
						return false;
					})
					.filter(p -> {
						try {
							BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class);
							if (attrs.isRegularFile() && !attrs.isSymbolicLink() && !attrs.isDirectory()) {
								long ctime = attrs.creationTime().toMillis();
								long atime = attrs.lastAccessTime().toMillis();
								long mtime = attrs.lastModifiedTime().toMillis();
								long lastTime = Math.max(Math.max(ctime, atime), mtime);
								long safeTime;
								if (p.getFileName().toString().startsWith("LOG.old.")) {
									safeTime = System.currentTimeMillis() - Duration.ofHours(24).toMillis();
								} else {
									safeTime = System.currentTimeMillis() - Duration.ofHours(12).toMillis();
								}
								if (lastTime < safeTime) {
									return true;
								}
							}
						} catch (IOException ex) {
							ex.printStackTrace();
							return false;
						}
						return false;
					})
					.forEach(path -> {
						try {
							Files.deleteIfExists(path);
							System.out.println("Deleted log file \"" + path + "\"");
						} catch (IOException e) {
							e.printStackTrace();
						}
					});
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}
