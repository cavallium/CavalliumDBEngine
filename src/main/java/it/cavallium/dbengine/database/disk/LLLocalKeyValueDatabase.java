package it.cavallium.dbengine.database.disk;

import io.netty.buffer.ByteBufAllocator;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.time.StopWatch;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DbPath;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RateLimiter;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WriteBufferManager;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LLLocalKeyValueDatabase implements LLKeyValueDatabase {

	static {
		RocksDB.loadLibrary();
	}

	protected static final Logger logger = LoggerFactory.getLogger(LLLocalKeyValueDatabase.class);
	private static final ColumnFamilyDescriptor DEFAULT_COLUMN_FAMILY = new ColumnFamilyDescriptor(
			RocksDB.DEFAULT_COLUMN_FAMILY);

	private final ByteBufAllocator allocator;
	private final Scheduler dbScheduler;

	// Configurations

	private final Path dbPath;
	private final String name;
	private final DatabaseOptions databaseOptions;

	private final boolean enableColumnsBug;
	private RocksDB db;
	private final Map<Column, ColumnFamilyHandle> handles;
	private final ConcurrentHashMap<Long, Snapshot> snapshotsHandles = new ConcurrentHashMap<>();
	private final AtomicLong nextSnapshotNumbers = new AtomicLong(1);

	@SuppressWarnings("SwitchStatementWithTooFewBranches")
	public LLLocalKeyValueDatabase(ByteBufAllocator allocator,
			String name,
			Path path,
			List<Column> columns,
			List<ColumnFamilyHandle> handles,
			DatabaseOptions databaseOptions) throws IOException {
		this.name = name;
		this.allocator = allocator;
		Options rocksdbOptions = openRocksDb(path, databaseOptions);
		try {
			List<ColumnFamilyDescriptor> descriptors = new LinkedList<>();
			descriptors
					.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
			for (Column column : columns) {
				descriptors
						.add(new ColumnFamilyDescriptor(column.name().getBytes(StandardCharsets.US_ASCII)));
			}

			// Get databases directory path
			Path databasesDirPath = path.toAbsolutePath().getParent();
			String dbPathString = databasesDirPath.toString() + File.separatorChar + path.getFileName();
			Path dbPath = Paths.get(dbPathString);
			this.dbPath = dbPath;

			// Set options
			this.databaseOptions = databaseOptions;

			int threadCap;
			if (databaseOptions.lowMemory()) {
				threadCap = Runtime.getRuntime().availableProcessors();
			} else {
				// 8 or more
				threadCap = Math.max(8, Runtime.getRuntime().availableProcessors());
			}
			this.dbScheduler = Schedulers.newBoundedElastic(threadCap,
					Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
					"db-" + name,
					60,
					true
			);
			this.enableColumnsBug = "true".equals(databaseOptions.extraFlags().getOrDefault("enableColumnBug", "false"));

			createIfNotExists(descriptors, rocksdbOptions, databaseOptions, dbPath, dbPathString);

			// Create all column families that don't exist
			createAllColumns(descriptors, rocksdbOptions, databaseOptions, dbPathString);

			while (true) {
				try {
					// a factory method that returns a RocksDB instance
					this.db = RocksDB.open(new DBOptions(rocksdbOptions),
							dbPathString,
							databaseOptions.inMemory() ? List.of(DEFAULT_COLUMN_FAMILY) : descriptors,
							handles
					);
					break;
				} catch (RocksDBException ex) {
					switch (ex.getMessage()) {
						case "Direct I/O is not supported by the specified DB." -> {
							logger.warn(ex.getLocalizedMessage());
							rocksdbOptions
									.setUseDirectReads(false)
									.setUseDirectIoForFlushAndCompaction(false)
									.setAllowMmapReads(databaseOptions.allowMemoryMapping())
									.setAllowMmapWrites(databaseOptions.allowMemoryMapping());
						}
						default -> throw ex;
					}
				}
			}
			createInMemoryColumns(descriptors, databaseOptions, handles);
			this.handles = new HashMap<>();
			if (enableColumnsBug && !databaseOptions.inMemory()) {
				for (int i = 0; i < columns.size(); i++) {
					this.handles.put(columns.get(i), handles.get(i));
				}
			} else {
				handles: for (ColumnFamilyHandle handle : handles) {
					for (Column column : columns) {
						if (Arrays.equals(column.name().getBytes(StandardCharsets.US_ASCII), handle.getName())) {
							this.handles.put(column, handle);
							continue handles;
						}
					}
				}
			}

			// compactDb(db, handles);
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

	@SuppressWarnings("unused")
	private void compactDb(RocksDB db, List<ColumnFamilyHandle> handles) {
		// force compact the database
		for (ColumnFamilyHandle cfh : handles) {
			var t = new Thread(() -> {
				int r = ThreadLocalRandom.current().nextInt();
				var s = StopWatch.createStarted();
				try {
					// Range rangeToCompact = db.suggestCompactRange(cfh);
					logger.info("Compacting range {}", r);
					db.compactRange(cfh, null, null, new CompactRangeOptions()
							.setAllowWriteStall(true)
							.setExclusiveManualCompaction(true)
							.setChangeLevel(false));
				} catch (RocksDBException e) {
					if ("Database shutdown".equalsIgnoreCase(e.getMessage())) {
						logger.warn("Compaction cancelled: database shutdown");
					} else {
						logger.warn("Failed to compact range", e);
					}
				}
				logger.info("Compacted range {} in {} milliseconds", r, s.getTime(TimeUnit.MILLISECONDS));
			}, "Compaction");
			t.setDaemon(true);
			t.start();
		}
		// end force compact
	}

	@SuppressWarnings({"CommentedOutCode", "PointlessArithmeticExpression"})
	private static Options openRocksDb(Path path, DatabaseOptions databaseOptions) throws IOException {
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
		options.setTargetFileSizeBase(64 * 1024 * 1024); // 64MiB sst file
		options.setTargetFileSizeMultiplier(2); // Each level is 2 times the previous level
		options.setCompressionPerLevel(List.of(CompressionType.NO_COMPRESSION,
				CompressionType.SNAPPY_COMPRESSION,
				CompressionType.SNAPPY_COMPRESSION
		));
		//options.setMaxBytesForLevelBase(4 * 256 * 1024 * 1024); // 4 times the sst file
		options.setManualWalFlush(false);
		options.setMinWriteBufferNumberToMerge(3);
		options.setMaxWriteBufferNumber(4);
		options.setAvoidFlushDuringShutdown(false); // Flush all WALs during shutdown
		options.setAvoidFlushDuringRecovery(false); // Flush all WALs during startup
		options.setWalRecoveryMode(databaseOptions.absoluteConsistency()
				? WALRecoveryMode.AbsoluteConsistency
				: WALRecoveryMode.PointInTimeRecovery); // Crash if the WALs are corrupted.Default: TolerateCorruptedTailRecords
		options.setDeleteObsoleteFilesPeriodMicros(20 * 1000000); // 20 seconds
		options.setPreserveDeletes(false);
		options.setKeepLogFileNum(10);
		options.setAllowFAllocate(true);
		options.setRateLimiter(new RateLimiter(10L * 1024L * 1024L)); // 10MiB/s max compaction write speed
		var paths = List.of(new DbPath(databasesDirPath.resolve(path.getFileName() + "_hot"),
						10L * 1024L * 1024L * 1024L), // 10GiB
				new DbPath(databasesDirPath.resolve(path.getFileName() + "_cold"),
						100L * 1024L * 1024L * 1024L), // 100GiB
				new DbPath(databasesDirPath.resolve(path.getFileName() + "_colder"),
						600L * 1024L * 1024L * 1024L)); // 600GiB
		options.setDbPaths(paths);
		options.setCfPaths(paths);
		// Direct I/O parameters. Removed because they use too much disk.
		//options.setUseDirectReads(true);
		//options.setUseDirectIoForFlushAndCompaction(true);
		//options.setWritableFileMaxBufferSize(1024 * 1024); // 1MB by default
		//options.setCompactionReadaheadSize(2 * 1024 * 1024); // recommend at least 2MB
		final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
		if (databaseOptions.lowMemory()) {
			// LOW MEMORY
			options
					.setLevelCompactionDynamicLevelBytes(false)
					.setBytesPerSync(0) // default
					.setWalBytesPerSync(0) // default
					.setIncreaseParallelism(1)
					.setMaxOpenFiles(15)
					.optimizeLevelStyleCompaction(1024 * 1024) // 1MiB of ram will be used for level style compaction
					.setWriteBufferSize(1024 * 1024) // 1MB
					.setWalTtlSeconds(0)
					.setWalSizeLimitMB(0) // 16MB
					.setMaxTotalWalSize(0) // automatic
			;
			tableOptions
					.setBlockCache(new LRUCache(8L * 1024L * 1024L)) // 8MiB
					.setCacheIndexAndFilterBlocks(false)
					.setPinL0FilterAndIndexBlocksInCache(false)
			;
			options.setWriteBufferManager(new WriteBufferManager(8L * 1024L * 1024L, new LRUCache(8L * 1024L * 1024L))); // 8MiB
		} else {
			// HIGH MEMORY
			options
					.setLevelCompactionDynamicLevelBytes(true)
					.setAllowConcurrentMemtableWrite(true)
					.setEnableWriteThreadAdaptiveYield(true)
					.setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
					.setBytesPerSync(1 * 1024 * 1024) // 1MiB
					.setWalBytesPerSync(10 * 1024 * 1024)
					.setMaxOpenFiles(150)
					.optimizeLevelStyleCompaction(
							128 * 1024 * 1024) // 128MiB of ram will be used for level style compaction
					.setWriteBufferSize(64 * 1024 * 1024) // 64MB
					.setWalTtlSeconds(30) // flush wal after 30 seconds
					.setWalSizeLimitMB(1024) // 1024MB
					.setMaxTotalWalSize(2L * 1024L * 1024L * 1024L) // 2GiB max wal directory size
			;
			tableOptions
					.setBlockCache(new LRUCache(128L * 1024L * 1024L)) // 128MiB
					.setCacheIndexAndFilterBlocks(true)
					.setPinL0FilterAndIndexBlocksInCache(true)
					;
			final BloomFilter bloomFilter = new BloomFilter(10, false);
			tableOptions.setOptimizeFiltersForMemory(true);
			tableOptions.setFilterPolicy(bloomFilter);
			options.setWriteBufferManager(new WriteBufferManager(256L * 1024L * 1024L, new LRUCache(128L * 1024L * 1024L))); // 128MiB

			if (databaseOptions.useDirectIO()) {
				options
						.setAllowMmapReads(false)
						.setAllowMmapWrites(false)
						.setUseDirectIoForFlushAndCompaction(true)
						.setUseDirectReads(true)
						// Option to enable readahead in compaction
						// If not set, it will be set to 2MB internally
						.setCompactionReadaheadSize(2 * 1024 * 1024) // recommend at least 2MB
						// Option to tune write buffer for direct writes
						.setWritableFileMaxBufferSize(1024 * 1024)
				;
			} else {
				options
						.setAllowMmapReads(databaseOptions.allowMemoryMapping())
						.setAllowMmapWrites(databaseOptions.allowMemoryMapping());
			}
		}

		tableOptions.setBlockSize(16 * 1024); // 16MiB
		options.setTableFormatConfig(tableOptions);
		options.setCompactionPriority(CompactionPriority.MinOverlappingRatio);

		return options;
	}

	private void createAllColumns(List<ColumnFamilyDescriptor> totalDescriptors,
			Options options,
			DatabaseOptions databaseOptions,
			String dbPathString) throws RocksDBException {
		if (databaseOptions.inMemory()) {
			return;
		}
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

	private void createInMemoryColumns(List<ColumnFamilyDescriptor> totalDescriptors,
			DatabaseOptions databaseOptions,
			List<ColumnFamilyHandle> handles)
			throws RocksDBException {
		if (!databaseOptions.inMemory()) {
			return;
		}
		List<byte[]> columnFamiliesToCreate = new LinkedList<>();

		for (ColumnFamilyDescriptor descriptor : totalDescriptors) {
			columnFamiliesToCreate.add(descriptor.getName());
		}

		for (byte[] name : columnFamiliesToCreate) {
			if (!Arrays.equals(name, DEFAULT_COLUMN_FAMILY.getName())) {
				var descriptor = new ColumnFamilyDescriptor(name);
				handles.add(db.createColumnFamily(descriptor));
			}
		}
	}

	private void createIfNotExists(List<ColumnFamilyDescriptor> descriptors,
			Options options,
			DatabaseOptions databaseOptions,
			Path dbPath,
			String dbPathString) throws RocksDBException {
		if (databaseOptions.inMemory()) {
			return;
		}
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
						getCfh(singletonListColumnName),
						(snapshot) -> snapshotsHandles.get(snapshot.getSequenceNumber()),
						LLLocalKeyValueDatabase.this.name,
						name,
						dbScheduler,
						defaultValue
				))
				.onErrorMap(cause -> new IOException("Failed to read " + Arrays.toString(name), cause))
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<LLLocalDictionary> getDictionary(byte[] columnName, UpdateMode updateMode) {
		return Mono
				.fromCallable(() -> new LLLocalDictionary(
						allocator,
						db,
						getCfh(columnName),
						name,
						Column.toString(columnName),
						dbScheduler,
						(snapshot) -> snapshotsHandles.get(snapshot.getSequenceNumber()),
						updateMode,
						databaseOptions
				))
				.subscribeOn(dbScheduler);
	}

	private ColumnFamilyHandle getCfh(byte[] columnName) throws RocksDBException {
		ColumnFamilyHandle cfh = handles.get(Column.special(Column.toString(columnName)));
		//noinspection RedundantIfStatement
		if (!enableColumnsBug) {
			assert Arrays.equals(cfh.getName(), columnName);
		}
		return cfh;
	}

	public DatabaseOptions getDatabaseOptions() {
		return databaseOptions;
	}

	@Override
	public Mono<Long> getProperty(String propertyName) {
		return Mono.fromCallable(() -> db.getAggregatedLongProperty(propertyName))
				.onErrorMap(cause -> new IOException("Failed to read " + propertyName, cause))
				.subscribeOn(dbScheduler);
	}

	@Override
	public Mono<Void> verifyChecksum() {
		return Mono
				.<Void>fromCallable(() -> {
					db.verifyChecksum();
					return null;
				})
				.onErrorMap(cause -> new IOException("Failed to verify checksum of database \""
						+ getDatabaseName() + "\"", cause))
				.subscribeOn(dbScheduler);
	}

	@Override
	public ByteBufAllocator getAllocator() {
		return allocator;
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
				.onErrorMap(cause -> new IOException("Failed to close", cause))
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
							logger.error("Error when deleting unused log files", ex);
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
