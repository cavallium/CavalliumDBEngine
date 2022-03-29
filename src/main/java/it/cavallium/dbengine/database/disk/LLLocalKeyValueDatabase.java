package it.cavallium.dbengine.database.disk;

import static io.netty5.buffer.api.StandardAllocationTypes.OFF_HEAP;
import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.util.internal.PlatformDependent;
import it.cavallium.dbengine.client.MemoryStats;
import it.cavallium.dbengine.database.ColumnUtils;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseLevel;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.DatabaseVolume;
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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ChecksumType;
import org.rocksdb.ClockCache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompressionOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DbPath;
import org.rocksdb.FlushOptions;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.TxnDBWritePolicy;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WriteBufferManager;
import org.rocksdb.util.SizeUnit;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LLLocalKeyValueDatabase implements LLKeyValueDatabase {

	private static final boolean DELETE_LOG_FILES = false;

	static {
		RocksDB.loadLibrary();
		LLUtils.initHooks();
	}

	protected static final Logger logger = LogManager.getLogger(LLLocalKeyValueDatabase.class);
	private static final ColumnFamilyDescriptor DEFAULT_COLUMN_FAMILY
			= new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);

	private final BufferAllocator allocator;
	private final MeterRegistry meterRegistry;
	private final Scheduler dbScheduler;

	private final Timer snapshotTime;

	// Configurations

	private final Path dbPath;
	private final String name;
	private final DatabaseOptions databaseOptions;
	private final boolean nettyDirect;

	private final boolean enableColumnsBug;
	private RocksDB db;
	private final Map<Column, ColumnFamilyHandle> handles;
	private final ConcurrentHashMap<Long, Snapshot> snapshotsHandles = new ConcurrentHashMap<>();
	private final AtomicLong nextSnapshotNumbers = new AtomicLong(1);
	private volatile boolean closed = false;

	@SuppressWarnings("SwitchStatementWithTooFewBranches")
	public LLLocalKeyValueDatabase(BufferAllocator allocator,
			MeterRegistry meterRegistry,
			String name,
			boolean inMemory,
			@Nullable Path path,
			List<Column> columns,
			List<ColumnFamilyHandle> handles,
			DatabaseOptions databaseOptions) throws IOException {
		this.name = name;
		this.allocator = allocator;
		this.nettyDirect = databaseOptions.allowNettyDirect() && allocator.getAllocationType() == OFF_HEAP;
		this.meterRegistry = meterRegistry;

		this.snapshotTime = Timer
				.builder("db.snapshot.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", name)
				.register(meterRegistry);

		if (nettyDirect) {
			if (!PlatformDependent.hasUnsafe()) {
				throw new UnsupportedOperationException("Please enable unsafe support or disable netty direct buffers",
						PlatformDependent.getUnsafeUnavailabilityCause()
				);
			}
		}

		OptionsWithCache optionsWithCache = openRocksDb(path, databaseOptions);
		var rocksdbOptions = optionsWithCache.options();
		try {
			List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();

			descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
			for (Column column : columns) {
				var columnOptions = new ColumnFamilyOptions();
				columnOptions.setMaxBytesForLevelBase(256 * SizeUnit.MB);
				columnOptions.setMaxBytesForLevelMultiplier(10);
				columnOptions.setLevelCompactionDynamicLevelBytes(true);
				columnOptions.setLevel0FileNumCompactionTrigger(2);
				columnOptions.setLevel0SlowdownWritesTrigger(20);
				columnOptions.setLevel0StopWritesTrigger(36);

				//noinspection ConstantConditions
				if (databaseOptions.memtableMemoryBudgetBytes() != null) {
					// 16MiB/256MiB of ram will be used for level style compaction
					columnOptions.optimizeLevelStyleCompaction(databaseOptions
							.memtableMemoryBudgetBytes()
							.orElse(databaseOptions.lowMemory() ? 16L * SizeUnit.MB : 128L * SizeUnit.MB));
				}

				if (!databaseOptions.levels().isEmpty()) {
					var firstLevelOptions = getRocksLevelOptions(databaseOptions.levels().get(0));
					columnOptions.setCompressionType(firstLevelOptions.compressionType);
					columnOptions.setCompressionOptions(firstLevelOptions.compressionOptions);

					var lastLevelOptions = getRocksLevelOptions(databaseOptions
							.levels()
							.get(databaseOptions.levels().size() - 1));
					columnOptions.setBottommostCompressionType(lastLevelOptions.compressionType);
					columnOptions.setBottommostCompressionOptions(lastLevelOptions.compressionOptions);

					columnOptions.setCompressionPerLevel(databaseOptions
							.levels()
							.stream()
							.map(v -> v.compression().getType())
							.toList());
				} else {
					columnOptions.setNumLevels(7);
					List<CompressionType> compressionTypes = new ArrayList<>(7);
					for (int i = 0; i < 7; i++) {
						if (i < 2) {
							compressionTypes.add(CompressionType.NO_COMPRESSION);
						} else {
							compressionTypes.add(CompressionType.LZ4_COMPRESSION);
						}
					}
					columnOptions.setBottommostCompressionType(CompressionType.LZ4_COMPRESSION);
					columnOptions.setBottommostCompressionOptions(new CompressionOptions()
							.setEnabled(true)
							.setMaxDictBytes(32768));
					columnOptions.setCompressionPerLevel(compressionTypes);
				}

				final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
				if (!databaseOptions.lowMemory()) {
					final BloomFilter bloomFilter = new BloomFilter(3, false);
					tableOptions.setFilterPolicy(bloomFilter);
					tableOptions.setOptimizeFiltersForMemory(true);
				}
				tableOptions
						.setPinTopLevelIndexAndFilter(true)
						.setPinL0FilterAndIndexBlocksInCache(true)
						.setCacheIndexAndFilterBlocksWithHighPriority(true)
						.setCacheIndexAndFilterBlocks(databaseOptions.setCacheIndexAndFilterBlocks().orElse(true))
						.setPartitionFilters(true)
						.setIndexType(IndexType.kTwoLevelIndexSearch)
						.setFormatVersion(5)
						//todo: replace with kxxhash3
						.setChecksumType(ChecksumType.kxxHash)
						.setBlockCacheCompressed(optionsWithCache.compressedCache())
						.setBlockCache(optionsWithCache.standardCache())
						.setBlockSize(512 * 1024); // 512KiB

				//columnOptions.setLevelCompactionDynamicLevelBytes(true);
				columnOptions.setTableFormatConfig(tableOptions);
				columnOptions.setCompactionPriority(CompactionPriority.OldestSmallestSeqFirst);

				descriptors.add(new ColumnFamilyDescriptor(column.name().getBytes(StandardCharsets.US_ASCII), columnOptions));
			}

			// Get databases directory path
			Objects.requireNonNull(path);
			Path databasesDirPath = path.toAbsolutePath().getParent();
			String dbPathString = databasesDirPath.toString() + File.separatorChar + path.getFileName();
			Path dbPath = Paths.get(dbPathString);
			this.dbPath = dbPath;

			// Set options
			this.databaseOptions = databaseOptions;

			int threadCap;
			if (databaseOptions.lowMemory()) {
				threadCap = Math.max(1, Runtime.getRuntime().availableProcessors());

				this.dbScheduler = Schedulers.boundedElastic();
			} else {
				// 8 or more
				threadCap = Math.max(8, Math.max(Runtime.getRuntime().availableProcessors(),
						Integer.parseInt(System.getProperty("it.cavallium.dbengine.scheduler.threads", "0"))));
				if (Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.scheduler.shared", "true"))) {
					this.dbScheduler = Schedulers.boundedElastic();
				} else {
					this.dbScheduler = Schedulers.newBoundedElastic(threadCap,
							Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
							"db-" + name,
							60,
							true
					);
				}
			}
			this.enableColumnsBug = "true".equals(databaseOptions.extraFlags().getOrDefault("enableColumnBug", "false"));

			createIfNotExists(descriptors, rocksdbOptions, inMemory, dbPath, dbPathString);

			while (true) {
				try {
					// a factory method that returns a RocksDB instance
					if (databaseOptions.optimistic()) {
						this.db = OptimisticTransactionDB.open(rocksdbOptions, dbPathString, descriptors, handles);
					} else {
						this.db = TransactionDB.open(rocksdbOptions,
								new TransactionDBOptions()
										.setWritePolicy(TxnDBWritePolicy.WRITE_COMMITTED)
										.setTransactionLockTimeout(5000)
										.setDefaultLockTimeout(5000),
								dbPathString,
								descriptors,
								handles
						);
					}
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
			this.handles = new HashMap<>();
			if (enableColumnsBug && !inMemory) {
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

		registerGauge(meterRegistry, name, "rocksdb.estimate-table-readers-mem");
		registerGauge(meterRegistry, name, "rocksdb.size-all-mem-tables");
		registerGauge(meterRegistry, name, "rocksdb.cur-size-all-mem-tables");
		registerGauge(meterRegistry, name, "rocksdb.estimate-num-keys");
		registerGauge(meterRegistry, name, "rocksdb.block-cache-usage");
		registerGauge(meterRegistry, name, "rocksdb.block-cache-pinned-usage");
	}

	public Map<Column, ColumnFamilyHandle> getAllColumnFamilyHandles() {
		return this.handles;
	}

	private record RocksLevelOptions(CompressionType compressionType, CompressionOptions compressionOptions) {}
	private RocksLevelOptions getRocksLevelOptions(DatabaseLevel levelOptions) {
		var compressionType = levelOptions.compression().getType();
		var compressionOptions = new CompressionOptions();
		if (compressionType != CompressionType.NO_COMPRESSION) {
			compressionOptions.setEnabled(true);
			compressionOptions.setMaxDictBytes(levelOptions.maxDictBytes());
		} else {
			compressionOptions.setEnabled(false);
		}
		return new RocksLevelOptions(compressionType, compressionOptions);
	}

	private void registerGauge(MeterRegistry meterRegistry, String name, String propertyName) {
		meterRegistry.gauge("rocksdb.property.value",
				List.of(Tag.of("db.name", name), Tag.of("db.property.name", propertyName)),
				db,
				database -> {
					if (closed) {
						return 0d;
					}
					try {
						return database.getAggregatedLongProperty(propertyName);
					} catch (RocksDBException e) {
						throw new RuntimeException(e);
					}
				}
		);
	}

	@Override
	public String getDatabaseName() {
		return name;
	}

	private void flushAndCloseDb(RocksDB db, List<ColumnFamilyHandle> handles)
			throws RocksDBException {
		flushDb(db, handles);

		for (ColumnFamilyHandle handle : handles) {
			try {
				handle.close();
			} catch (Exception ex) {
				logger.error("Can't close column family", ex);
			}
		}
		try {
			db.closeE();
		} catch (RocksDBException ex) {
			if ("Cannot close DB with unreleased snapshot.".equals(ex.getMessage())) {
				snapshotsHandles.forEach((id, snapshot) -> {
					try {
						db.releaseSnapshot(snapshot);
					} catch (Exception ex2) {
						// ignore exception
						logger.debug("Failed to release snapshot " + id, ex2);
					}
				});
				db.closeE();
			}
			throw ex;
		}
	}

	private void flushDb(RocksDB db, List<ColumnFamilyHandle> handles) throws RocksDBException {
		if (Schedulers.isInNonBlockingThread()) {
			logger.error("Called flushDb in a nonblocking thread");
		}
		// force flush the database
		try (var flushOptions = new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true)) {
			db.flush(flushOptions);
		}
		try (var flushOptions = new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true)) {
			db.flush(flushOptions, handles);
		}
		db.flushWal(true);
		db.syncWal();
		// end force flush
	}

	@SuppressWarnings("unused")
	private void compactDb(TransactionDB db, List<ColumnFamilyHandle> handles) {
		if (Schedulers.isInNonBlockingThread()) {
			logger.error("Called compactDb in a nonblocking thread");
		}
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

	record OptionsWithCache(DBOptions options, @Nullable Cache standardCache, @Nullable Cache compressedCache) {}

	private static OptionsWithCache openRocksDb(@Nullable Path path, DatabaseOptions databaseOptions) throws IOException {
		// Get databases directory path
		Path databasesDirPath;
		if (path != null) {
			databasesDirPath = path.toAbsolutePath().getParent();
			// Create base directories
			if (Files.notExists(databasesDirPath)) {
				Files.createDirectories(databasesDirPath);
			}
		} else {
			databasesDirPath = null;
		}

		// the Options class contains a set of configurable DB options
		// that determines the behaviour of the database.
		var options = new DBOptions();
		options.setEnablePipelinedWrite(true);
		options.setMaxSubcompactions(2);
		var customWriteRate = Long.parseLong(System.getProperty("it.cavallium.dbengine.write.delayedrate", "-1"));
		if (customWriteRate >= 0) {
			options.setDelayedWriteRate(customWriteRate);
		} else {
			options.setDelayedWriteRate(64 * SizeUnit.MB);
		}
		options.setCreateIfMissing(true);
		options.setSkipStatsUpdateOnDbOpen(true);
		options.setCreateMissingColumnFamilies(true);
		options.setInfoLogLevel(InfoLogLevel.WARN_LEVEL);
		options.setAvoidFlushDuringShutdown(false); // Flush all WALs during shutdown
		options.setAvoidFlushDuringRecovery(true); // Flush all WALs during startup
		options.setWalRecoveryMode(databaseOptions.absoluteConsistency()
				? WALRecoveryMode.AbsoluteConsistency
				: WALRecoveryMode.PointInTimeRecovery); // Crash if the WALs are corrupted.Default: TolerateCorruptedTailRecords
		options.setDeleteObsoleteFilesPeriodMicros(20 * 1000000); // 20 seconds
		options.setKeepLogFileNum(10);

		Objects.requireNonNull(databasesDirPath);
		Objects.requireNonNull(path.getFileName());
		List<DbPath> paths = convertPaths(databasesDirPath, path.getFileName(), databaseOptions.volumes());
		options.setDbPaths(paths);
		options.setMaxOpenFiles(databaseOptions.maxOpenFiles().orElse(-1));

		Cache blockCache;
		Cache compressedCache;
		if (databaseOptions.lowMemory()) {
			// LOW MEMORY
			options
					.setBytesPerSync(0) // default
					.setWalBytesPerSync(0) // default
					.setIncreaseParallelism(1)
					.setDbWriteBufferSize(8 * SizeUnit.MB)
					.setWalTtlSeconds(0)
					.setWalSizeLimitMB(0) // 16MB
					.setMaxTotalWalSize(0) // automatic
			;
			blockCache = new ClockCache(databaseOptions.blockCache().orElse( 8L * SizeUnit.MB) / 2, -1, true);
			compressedCache = null;

			if (databaseOptions.useDirectIO()) {
				options
						// Option to enable readahead in compaction
						// If not set, it will be set to 2MB internally
						.setCompactionReadaheadSize(2 * 1024 * 1024) // recommend at least 2MB
						// Option to tune write buffer for direct writes
						.setWritableFileMaxBufferSize(1024 * 1024)
				;
			}
		} else {
			// HIGH MEMORY
			options
					.setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
					.setDbWriteBufferSize(64 * SizeUnit.MB)
					.setBytesPerSync(64 * SizeUnit.KB)
					.setWalBytesPerSync(64 * SizeUnit.KB)

					.setWalTtlSeconds(30) // flush wal after 30 seconds
					.setWalSizeLimitMB(1024) // 1024MB
					.setMaxTotalWalSize(2L * SizeUnit.GB) // 2GiB max wal directory size
			;
			blockCache = new ClockCache(databaseOptions.blockCache().orElse( 512 * SizeUnit.MB) / 2);
			compressedCache = null;

			if (databaseOptions.useDirectIO()) {
				options
						// Option to enable readahead in compaction
						// If not set, it will be set to 2MB internally
						.setCompactionReadaheadSize(4 * 1024 * 1024) // recommend at least 2MB
						// Option to tune write buffer for direct writes
						.setWritableFileMaxBufferSize(4 * 1024 * 1024)
				;
			}
		}

		options.setWriteBufferManager(new WriteBufferManager(256L * 1024L * 1024L, blockCache));

		if (databaseOptions.useDirectIO()) {
			options
					.setAllowMmapReads(false)
					.setAllowMmapWrites(false)
					.setUseDirectReads(true)
			;
		} else {
			options
					.setAllowMmapReads(databaseOptions.allowMemoryMapping())
					.setAllowMmapWrites(databaseOptions.allowMemoryMapping());
		}

		if (!databaseOptions.allowMemoryMapping()) {
			options.setUseDirectIoForFlushAndCompaction(true);
		}

		return new OptionsWithCache(options, blockCache, compressedCache);
	}

	private static List<DbPath> convertPaths(Path databasesDirPath, Path path, List<DatabaseVolume> volumes) {
		var paths = new ArrayList<DbPath>(volumes.size());
		if (volumes.isEmpty()) {
			return List.of(new DbPath(databasesDirPath.resolve(path.getFileName() + "_hot"),
							100L * 1024L * 1024L * 1024L), // 100GiB
					new DbPath(databasesDirPath.resolve(path.getFileName() + "_cold"),
							500L * 1024L * 1024L * 1024L), // 500GiB
					new DbPath(databasesDirPath.resolve(path.getFileName() + "_colder"),
							500L * 1024L * 1024L * 1024L)); // 500GiB
		}
		for (DatabaseVolume volume : volumes) {
			Path volumePath;
			if (volume.volumePath().isAbsolute()) {
				volumePath = volume.volumePath();
			} else {
				volumePath = databasesDirPath.resolve(volume.volumePath());
			}
			paths.add(new DbPath(volumePath, volume.targetSizeBytes()));
		}
		return paths;
	}

	private void createIfNotExists(List<ColumnFamilyDescriptor> descriptors,
			DBOptions options,
			boolean inMemory,
			Path dbPath,
			String dbPathString) throws RocksDBException {
		if (inMemory) {
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

			this.db = RocksDB.open(new DBOptions(options).setCreateMissingColumnFamilies(true),
					dbPathString,
					descriptors,
					handles
			);

			flushAndCloseDb(db, handles);
			this.db = null;
		}
	}

	@Override
	public Mono<LLLocalSingleton> getSingleton(byte[] singletonListColumnName,
			byte[] name,
			byte @Nullable[] defaultValue) {
		return Mono
				.fromCallable(() -> new LLLocalSingleton(
						getRocksDBColumn(db, getCfh(singletonListColumnName)),
						(snapshot) -> snapshotsHandles.get(snapshot.getSequenceNumber()),
						LLLocalKeyValueDatabase.this.name,
						name,
						ColumnUtils.toString(singletonListColumnName),
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
						getRocksDBColumn(db, getCfh(columnName)),
						name,
						ColumnUtils.toString(columnName),
						dbScheduler,
						(snapshot) -> snapshotsHandles.get(snapshot.getSequenceNumber()),
						updateMode,
						databaseOptions
				))
				.subscribeOn(dbScheduler);
	}

	public RocksDBColumn getRocksDBColumn(byte[] columnName) {
		ColumnFamilyHandle cfh;
		try {
			cfh = getCfh(columnName);
		} catch (RocksDBException e) {
			throw new UnsupportedOperationException("Column family doesn't exist: " + Arrays.toString(columnName), e);
		}
		return getRocksDBColumn(db, cfh);
	}

	private RocksDBColumn getRocksDBColumn(RocksDB db, ColumnFamilyHandle cfh) {
		if (db instanceof OptimisticTransactionDB optimisticTransactionDB) {
			return new OptimisticRocksDBColumn(optimisticTransactionDB, databaseOptions, allocator, cfh, meterRegistry);
		} else if (db instanceof TransactionDB transactionDB) {
			return new PessimisticRocksDBColumn(transactionDB, databaseOptions, allocator, cfh, meterRegistry);
		} else {
			return new StandardRocksDBColumn(db, databaseOptions, allocator, cfh, meterRegistry);
		}
	}

	private ColumnFamilyHandle getCfh(byte[] columnName) throws RocksDBException {
		ColumnFamilyHandle cfh = handles.get(ColumnUtils.special(ColumnUtils.toString(columnName)));
		assert enableColumnsBug || Arrays.equals(cfh.getName(), columnName);
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
	public Mono<MemoryStats> getMemoryStats() {
		return Mono
				.fromCallable(() -> new MemoryStats(db.getAggregatedLongProperty("rocksdb.estimate-table-readers-mem"),
						db.getAggregatedLongProperty("rocksdb.size-all-mem-tables"),
						db.getAggregatedLongProperty("rocksdb.cur-size-all-mem-tables"),
						db.getAggregatedLongProperty("rocksdb.estimate-num-keys"),
						db.getAggregatedLongProperty("rocksdb.block-cache-usage"),
						db.getAggregatedLongProperty("rocksdb.block-cache-pinned-usage")
				))
				.onErrorMap(cause -> new IOException("Failed to read memory stats", cause))
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
	public BufferAllocator getAllocator() {
		return allocator;
	}

	@Override
	public MeterRegistry getMeterRegistry() {
		return meterRegistry;
	}

	@Override
	public Mono<LLSnapshot> takeSnapshot() {
		return Mono
				.fromCallable(() -> snapshotTime.recordCallable(() -> {
					var snapshot = db.getSnapshot();
					long currentSnapshotSequenceNumber = nextSnapshotNumbers.getAndIncrement();
					this.snapshotsHandles.put(currentSnapshotSequenceNumber, snapshot);
					return new LLSnapshot(currentSnapshotSequenceNumber);
				}))
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
						closed = true;
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
		if (!DELETE_LOG_FILES) {
			return;
		}
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
							logger.error(MARKER_ROCKSDB, "Failed to delete log file \"" + path + "\"", e);
						}
					});
		} catch (IOException ex) {
			logger.error(MARKER_ROCKSDB, "Failed to delete unused log files", ex);
		}
	}
}
