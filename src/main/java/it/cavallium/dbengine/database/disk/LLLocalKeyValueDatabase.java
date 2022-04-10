package it.cavallium.dbengine.database.disk;

import static io.netty5.buffer.api.StandardAllocationTypes.OFF_HEAP;
import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static org.rocksdb.ColumnFamilyOptionsInterface.DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET;

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
import it.cavallium.dbengine.database.TableWithProperties;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.ColumnOptions;
import it.cavallium.dbengine.rpc.current.data.DatabaseLevel;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.DatabaseVolume;
import it.cavallium.dbengine.rpc.current.data.NamedColumnOptions;
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
import java.util.stream.Collectors;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ChecksumType;
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
import org.rocksdb.LRUCache;
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
import org.warp.commonutils.type.ShortNamedThreadFactory;
import reactor.core.publisher.Flux;
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
	private final Scheduler dbWScheduler;
	private final Scheduler dbRScheduler;

	private final Timer snapshotTime;

	// Configurations

	private final Path dbPath;
	private final String name;
	private final DatabaseOptions databaseOptions;
	private final boolean nettyDirect;

	private final boolean enableColumnsBug;
	private RocksDB db;
	private Cache standardCache;
	private Cache compressedCache;
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

			// Check column names validity
			for (NamedColumnOptions columnOption : databaseOptions.columnOptions()) {
				if (columns.stream().map(Column::name).noneMatch(columnName -> columnName.equals(columnOption.columnName()))) {
					throw new IllegalArgumentException(
							"Column " + columnOption.columnName() + " does not exist. Available columns: " + columns
									.stream()
									.map(Column::name)
									.collect(Collectors.joining(", ", "[", "]")));
				}
			}

			for (Column column : columns) {
				var columnFamilyOptions = new ColumnFamilyOptions();

				var columnOptions = databaseOptions
						.columnOptions()
						.stream()
						.filter(opts -> opts.columnName().equals(column.name()))
						.findFirst()
						.map(opts -> (ColumnOptions) opts)
						.orElse(databaseOptions.defaultColumnOptions());

				//noinspection ConstantConditions
				if (columnOptions.memtableMemoryBudgetBytes() != null) {
					// about 512MB of ram will be used for level style compaction
					columnFamilyOptions.optimizeLevelStyleCompaction(columnOptions.memtableMemoryBudgetBytes().orElse(
							databaseOptions.lowMemory()
									? (DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET / 4)
									: DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET));
				}

				// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
				// https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
				columnFamilyOptions.setMaxBytesForLevelBase((databaseOptions.spinning() ? 1024 : 256) * SizeUnit.MB);
				// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
				columnFamilyOptions.setMaxBytesForLevelMultiplier(10);
				// This option is not supported with multiple db paths
				if (databaseOptions.volumes().size() <= 1) {
					// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
					// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
					columnFamilyOptions.setLevelCompactionDynamicLevelBytes(true);
				}
				// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
				columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);
				// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
				columnFamilyOptions.setLevel0SlowdownWritesTrigger(20);
				// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
				columnFamilyOptions.setLevel0StopWritesTrigger(36);

				if (!columnOptions.levels().isEmpty()) {
					var firstLevelOptions = getRocksLevelOptions(columnOptions.levels().get(0));
					columnFamilyOptions.setCompressionType(firstLevelOptions.compressionType);
					columnFamilyOptions.setCompressionOptions(firstLevelOptions.compressionOptions);

					var lastLevelOptions = getRocksLevelOptions(columnOptions
							.levels()
							.get(columnOptions.levels().size() - 1));
					columnFamilyOptions.setBottommostCompressionType(lastLevelOptions.compressionType);
					columnFamilyOptions.setBottommostCompressionOptions(lastLevelOptions.compressionOptions);

					columnFamilyOptions.setCompressionPerLevel(columnOptions
							.levels()
							.stream()
							.map(v -> v.compression().getType())
							.toList());
				} else {
					columnFamilyOptions.setNumLevels(7);
					List<CompressionType> compressionTypes = new ArrayList<>(7);
					for (int i = 0; i < 7; i++) {
						if (i < 2) {
							compressionTypes.add(CompressionType.NO_COMPRESSION);
						} else {
							compressionTypes.add(CompressionType.LZ4_COMPRESSION);
						}
					}
					columnFamilyOptions.setBottommostCompressionType(CompressionType.LZ4_COMPRESSION);
					columnFamilyOptions.setBottommostCompressionOptions(new CompressionOptions()
							.setEnabled(true)
							.setMaxDictBytes(32768));
					columnFamilyOptions.setCompressionPerLevel(compressionTypes);
				}

				final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
				if (!databaseOptions.lowMemory()) {
					tableOptions.setOptimizeFiltersForMemory(true);
				}
				tableOptions.setVerifyCompression(false);
				if (columnOptions.filter().isPresent()) {
					var filterOptions = columnOptions.filter().get();

					if (filterOptions instanceof it.cavallium.dbengine.rpc.current.data.BloomFilter bloomFilterOptions) {
						// If OptimizeFiltersForHits == true: memory size = bitsPerKey * (totalKeys * 0.1)
						// If OptimizeFiltersForHits == false: memory size = bitsPerKey * totalKeys
						final BloomFilter bloomFilter = new BloomFilter(bloomFilterOptions.bitsPerKey());
						tableOptions.setFilterPolicy(bloomFilter);
					}
				}
				boolean cacheIndexAndFilterBlocks = columnOptions.cacheIndexAndFilterBlocks()
						// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
						.orElse(true);
				if (databaseOptions.spinning()) {
					// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
					cacheIndexAndFilterBlocks = true;
					// https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
					columnFamilyOptions.setMinWriteBufferNumberToMerge(3);
					// https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
					columnFamilyOptions.setMaxWriteBufferNumber(4);
				}
				tableOptions
						// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
						.setPinTopLevelIndexAndFilter(true)
						// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
						.setPinL0FilterAndIndexBlocksInCache(true)
						// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
						.setCacheIndexAndFilterBlocksWithHighPriority(true)
						.setCacheIndexAndFilterBlocks(cacheIndexAndFilterBlocks)
						// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
						.setPartitionFilters(true)
						// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
						.setIndexType(IndexType.kTwoLevelIndexSearch)
						//todo: replace with kxxhash3
						.setChecksumType(ChecksumType.kxxHash)
						// Spinning disks: 64KiB to 256KiB (also 512KiB). SSDs: 16KiB
						// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
						// https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
						.setBlockSize(columnOptions.blockSize().orElse((databaseOptions.spinning() ? 128 : 16) * 1024))
						.setBlockCacheCompressed(optionsWithCache.compressedCache())
						.setBlockCache(optionsWithCache.standardCache());

				columnFamilyOptions.setTableFormatConfig(tableOptions);
				columnFamilyOptions.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
				if (columnOptions.filter().isPresent()) {
					var filterOptions = columnOptions.filter().get();

					if (filterOptions instanceof it.cavallium.dbengine.rpc.current.data.BloomFilter bloomFilterOptions) {
						boolean optimizeForHits = bloomFilterOptions.optimizeForHits()
								// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
								// https://github.com/EighteenZi/rocksdb_wiki/blob/master/RocksDB-Tuning-Guide.md#throughput-gap-between-random-read-vs-sequential-read-is-much-higher-in-spinning-disks-suggestions=
								.orElse(databaseOptions.spinning());
						columnFamilyOptions.setOptimizeFiltersForHits(optimizeForHits);
					}
				}

				// // Increasing this value can reduce the frequency of compaction and reduce write amplification,
				// // but it will also cause old data to be unable to be cleaned up in time, thus increasing read amplification.
				// // This parameter is not easy to adjust. It is generally not recommended to set it above 256MB.
				// https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
				columnFamilyOptions.setTargetFileSizeBase((databaseOptions.spinning() ? 256 : 64) * SizeUnit.MB);
				// // For each level up, the threshold is multiplied by the factor target_file_size_multiplier
				// // (but the default value is 1, which means that the maximum sstable of each level is the same).
				columnFamilyOptions.setTargetFileSizeMultiplier(1);

				descriptors.add(new ColumnFamilyDescriptor(column.name().getBytes(StandardCharsets.US_ASCII), columnFamilyOptions));
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

				this.dbWScheduler = Schedulers.boundedElastic();
				this.dbRScheduler = Schedulers.boundedElastic();
			} else {
				// 8 or more
				threadCap = Math.max(8, Runtime.getRuntime().availableProcessors());
				{
					var threadCapProperty = Integer.parseInt(System.getProperty("it.cavallium.dbengine.scheduler.write.threads", "0"));
					if (threadCapProperty > 1) {
						threadCap = threadCapProperty;
					}
				}
				if (Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.scheduler.write.shared", "true"))) {
					this.dbWScheduler = Schedulers.boundedElastic();
				} else {
					this.dbWScheduler = Schedulers.newBoundedElastic(threadCap,
							Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
							new ShortNamedThreadFactory("db-write-" + name).setDaemon(true).withGroup(new ThreadGroup("database-write")),
							60
					);
				}
				// 8 or more
				threadCap = Math.max(8, Runtime.getRuntime().availableProcessors());
				{
					var threadCapProperty = Integer.parseInt(System.getProperty("it.cavallium.dbengine.scheduler.read.threads", "0"));
					if (threadCapProperty > 1) {
						threadCap = threadCapProperty;
					}
				}
				if (Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.scheduler.read.shared", "true"))) {
					this.dbRScheduler = Schedulers.boundedElastic();
				} else {
					this.dbRScheduler = Schedulers.newBoundedElastic(threadCap,
							Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
							new ShortNamedThreadFactory("db-write-" + name).setDaemon(true).withGroup(new ThreadGroup("database-read")),
							60
					);
				}
			}
			this.enableColumnsBug = "true".equals(databaseOptions.extraFlags().getOrDefault("enableColumnBug", "false"));

			createIfNotExists(descriptors, rocksdbOptions, standardCache, compressedCache, inMemory, dbPath, dbPathString);

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
					this.standardCache = optionsWithCache.standardCache;
					this.compressedCache = optionsWithCache.compressedCache;
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

		try {
			for (ColumnFamilyHandle cfh : handles) {
				var props = db.getProperty(cfh, "rocksdb.stats");
				logger.trace("Stats for database {}, column {}: {}",
						name,
						new String(cfh.getName(), StandardCharsets.UTF_8),
						props
				);
			}
		} catch (RocksDBException ex) {
			logger.debug("Failed to obtain stats", ex);
		}

		registerGauge(meterRegistry, name, "rocksdb.estimate-table-readers-mem", false);
		registerGauge(meterRegistry, name, "rocksdb.size-all-mem-tables", false);
		registerGauge(meterRegistry, name, "rocksdb.cur-size-all-mem-tables", false);
		registerGauge(meterRegistry, name, "rocksdb.estimate-num-keys", false);
		registerGauge(meterRegistry, name, "rocksdb.block-cache-usage", true);
		registerGauge(meterRegistry, name, "rocksdb.block-cache-pinned-usage", true);
		// Bloom seek stats
		registerGauge(meterRegistry, name, "rocksdb.bloom.filter.prefix.useful", false);
		registerGauge(meterRegistry, name, "rocksdb.bloom.filter.prefix.checked", false);
		// Bloom point lookup stats
		registerGauge(meterRegistry, name, "rocksdb.bloom.filter.useful", false);
		registerGauge(meterRegistry, name, "rocksdb.bloom.filter.full.positive", false);
		registerGauge(meterRegistry, name, "rocksdb.bloom.filter.full.true.positive", false);
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

	private void registerGauge(MeterRegistry meterRegistry, String name, String propertyName, boolean divideByAllColumns) {
		meterRegistry.gauge("rocksdb.property.value",
				List.of(Tag.of("db.name", name), Tag.of("db.property.name", propertyName)),
				db,
				database -> {
					if (closed) {
						return 0d;
					}
					try {
						return database.getAggregatedLongProperty(propertyName)
								/ (divideByAllColumns ? getAllColumnFamilyHandles().size() : 1d);
					} catch (RocksDBException e) {
						if ("NotFound".equals(e.getMessage())) {
							return 0d;
						}
						throw new RuntimeException(e);
					}
				}
		);
	}

	@Override
	public String getDatabaseName() {
		return name;
	}

	private void flushAndCloseDb(RocksDB db, Cache standardCache, Cache compressedCache, List<ColumnFamilyHandle> handles)
			throws RocksDBException {
		if (db.isOwningHandle()) {
			flushDb(db, handles);
		}

		for (ColumnFamilyHandle handle : handles) {
			try {
				handle.close();
			} catch (Exception ex) {
				logger.error("Can't close column family", ex);
			}
		}
		snapshotsHandles.forEach((id, snapshot) -> {
			try {
				if (db.isOwningHandle() && snapshot.isOwningHandle()) {
					db.releaseSnapshot(snapshot);
				}
			} catch (Exception ex2) {
				// ignore exception
				logger.debug("Failed to release snapshot " + id, ex2);
			}
		});
		db.closeE();
		compressedCache.close();
		standardCache.close();
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
		if (databaseOptions.spinning()) {
			// https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
			options.setUseFsync(false);
		}

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
					.setWalSizeLimitMB(0)
					.setMaxTotalWalSize(0) // automatic
			;
			// DO NOT USE ClockCache! IT'S BROKEN!
			blockCache = new LRUCache(databaseOptions.blockCache().orElse( 8L * SizeUnit.MB));
			compressedCache = new LRUCache(databaseOptions.compressedBlockCache().orElse( 8L * SizeUnit.MB));

			if (databaseOptions.spinning()) {
				options
						// method documentation
						.setCompactionReadaheadSize(16 * SizeUnit.MB)
						// guessed
						.setWritableFileMaxBufferSize(16 * SizeUnit.MB);
			}
			if (databaseOptions.useDirectIO()) {
				options
						// Option to enable readahead in compaction
						// If not set, it will be set to 2MB internally
						.setCompactionReadaheadSize(2 * SizeUnit.MB) // recommend at least 2MB
						// Option to tune write buffer for direct writes
						.setWritableFileMaxBufferSize(2 * SizeUnit.MB)
				;
			}
		} else {
			// HIGH MEMORY
			options
					.setDbWriteBufferSize(64 * SizeUnit.MB)
					.setBytesPerSync(64 * SizeUnit.KB)
					.setWalBytesPerSync(64 * SizeUnit.KB)

					.setWalTtlSeconds(0)
					.setWalSizeLimitMB(0)
					.setMaxTotalWalSize(80 * SizeUnit.MB) // 80MiB max wal directory size
			;
			// DO NOT USE ClockCache! IT'S BROKEN!
			blockCache = new LRUCache(databaseOptions.blockCache().orElse( 512 * SizeUnit.MB));
			compressedCache = new LRUCache(databaseOptions.compressedBlockCache().orElse( 512 * SizeUnit.MB));

			if (databaseOptions.useDirectIO()) {
				options
						// Option to enable readahead in compaction
						// If not set, it will be set to 2MB internally
						.setCompactionReadaheadSize(4 * 1024 * 1024) // recommend at least 2MB
						// Option to tune write buffer for direct writes
						.setWritableFileMaxBufferSize(4 * 1024 * 1024)
				;
			}
			options.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
		}

		options.setRowCache(blockCache);
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
							0), // Legacy
					new DbPath(databasesDirPath.resolve(path.getFileName() + "_colder"),
							0)
			); // Legacy
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
			Cache standardCache,
			Cache compressedCache,
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

			this.db = RocksDB.open(options.setCreateMissingColumnFamilies(true),
					dbPathString,
					descriptors,
					handles
			);
			this.standardCache = standardCache;
			this.compressedCache = compressedCache;

			flushAndCloseDb(db, standardCache, compressedCache, handles);
			this.db = null;
			this.standardCache = null;
			this.compressedCache = null;
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
						dbWScheduler, dbRScheduler, defaultValue
				))
				.onErrorMap(cause -> new IOException("Failed to read " + Arrays.toString(name), cause))
				.subscribeOn(dbRScheduler);
	}

	@Override
	public Mono<LLLocalDictionary> getDictionary(byte[] columnName, UpdateMode updateMode) {
		return Mono
				.fromCallable(() -> new LLLocalDictionary(
						allocator,
						getRocksDBColumn(db, getCfh(columnName)),
						name,
						ColumnUtils.toString(columnName),
						dbWScheduler,
						dbRScheduler,
						(snapshot) -> snapshotsHandles.get(snapshot.getSequenceNumber()),
						updateMode,
						databaseOptions
				))
				.subscribeOn(dbRScheduler);
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
		var nettyDirect = databaseOptions.allowNettyDirect();
		if (db instanceof OptimisticTransactionDB optimisticTransactionDB) {
			return new OptimisticRocksDBColumn(optimisticTransactionDB, nettyDirect, allocator, name, cfh, meterRegistry);
		} else if (db instanceof TransactionDB transactionDB) {
			return new PessimisticRocksDBColumn(transactionDB, nettyDirect, allocator, name, cfh, meterRegistry);
		} else {
			return new StandardRocksDBColumn(db, nettyDirect, allocator, name, cfh, meterRegistry);
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
				.subscribeOn(dbRScheduler);
	}

	@Override
	public Mono<MemoryStats> getMemoryStats() {
		return Mono
				.fromCallable(() -> {
					if (!closed) {
						return new MemoryStats(db.getAggregatedLongProperty("rocksdb.estimate-table-readers-mem"),
								db.getAggregatedLongProperty("rocksdb.size-all-mem-tables"),
								db.getAggregatedLongProperty("rocksdb.cur-size-all-mem-tables"),
								db.getAggregatedLongProperty("rocksdb.estimate-num-keys"),
								db.getAggregatedLongProperty("rocksdb.block-cache-usage"),
								db.getAggregatedLongProperty("rocksdb.block-cache-pinned-usage")
						);
					} else {
						return null;
					}
				})
				.onErrorMap(cause -> new IOException("Failed to read memory stats", cause))
				.subscribeOn(dbRScheduler);
	}

	@Override
	public Mono<String> getRocksDBStats() {
		return Mono
				.fromCallable(() -> {
					if (!closed) {
						StringBuilder aggregatedStats = new StringBuilder();
						for (var entry : this.handles.entrySet()) {
							aggregatedStats
									.append(entry.getKey().name())
									.append("\n")
									.append(db.getProperty(entry.getValue(), "rocksdb.stats"))
									.append("\n");
						}
						return aggregatedStats.toString();
					} else {
						return null;
					}
				})
				.onErrorMap(cause -> new IOException("Failed to read stats", cause))
				.subscribeOn(dbRScheduler);
	}

	@Override
	public Flux<TableWithProperties> getTableProperties() {
		return Flux
				.fromIterable(handles.entrySet())
				.flatMapSequential(handle -> Mono
						.fromCallable(() -> {
							if (!closed) {
								return db.getPropertiesOfAllTables(handle.getValue());
							} else {
								return null;
							}
						})
						.subscribeOn(dbRScheduler)
						.flatMapIterable(Map::entrySet)
						.map(entry -> new TableWithProperties(handle.getKey().name(), entry.getKey(), entry.getValue()))
				)
				.onErrorMap(cause -> new IOException("Failed to read stats", cause));
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
				.subscribeOn(dbRScheduler);
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
				.subscribeOn(dbRScheduler);
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono
				.<Void>fromCallable(() -> {
					Snapshot dbSnapshot = this.snapshotsHandles.remove(snapshot.getSequenceNumber());
					if (dbSnapshot == null) {
						throw new IOException("Snapshot " + snapshot.getSequenceNumber() + " not found!");
					}
					if (!db.isOwningHandle()) {
						return null;
					}
					if (!dbSnapshot.isOwningHandle()) {
						return null;
					}
					db.releaseSnapshot(dbSnapshot);
					return null;
				})
				.subscribeOn(dbRScheduler);
	}

	@Override
	public Mono<Void> close() {
		return Mono
				.<Void>fromCallable(() -> {
					try {
						closed = true;
						flushAndCloseDb(db, standardCache, compressedCache, new ArrayList<>(handles.values()));
						deleteUnusedOldLogFiles();
					} catch (RocksDBException e) {
						throw new IOException(e);
					}
					return null;
				})
				.onErrorMap(cause -> new IOException("Failed to close", cause))
				.subscribeOn(dbWScheduler);
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
