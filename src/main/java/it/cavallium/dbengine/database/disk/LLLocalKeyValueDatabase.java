package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.mapList;
import static it.cavallium.dbengine.utils.StreamUtils.collect;
import static it.cavallium.dbengine.utils.StreamUtils.iterating;
import static java.lang.Boolean.parseBoolean;
import static java.util.Objects.requireNonNull;
import static org.rocksdb.ColumnFamilyOptionsInterface.DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import it.cavallium.datagen.nativedata.NullableString;
import it.cavallium.dbengine.client.Backuppable;
import it.cavallium.dbengine.client.MemoryStats;
import it.cavallium.dbengine.database.ColumnProperty;
import it.cavallium.dbengine.database.ColumnUtils;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.RocksDBLongProperty;
import it.cavallium.dbengine.database.RocksDBMapProperty;
import it.cavallium.dbengine.database.RocksDBStringProperty;
import it.cavallium.dbengine.database.TableWithProperties;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.ColumnOptions;
import it.cavallium.dbengine.rpc.current.data.DatabaseLevel;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.DatabaseVolume;
import it.cavallium.dbengine.rpc.current.data.NamedColumnOptions;
import it.cavallium.dbengine.rpc.current.data.NoFilter;
import it.cavallium.dbengine.utils.StreamUtils;
import java.io.File;
import java.io.IOException;
import it.cavallium.dbengine.utils.DBException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractImmutableNativeReference;
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
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.DbPath;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.PersistentCache;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.PrepopulateBlobCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.TickerType;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.TxnDBWritePolicy;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WriteBufferManager;
import org.rocksdb.util.SizeUnit;

public class LLLocalKeyValueDatabase extends Backuppable implements LLKeyValueDatabase {

	private static final boolean DELETE_LOG_FILES = false;
	private static final boolean FOLLOW_ROCKSDB_OPTIMIZATIONS = true;
	private static final boolean USE_CLOCK_CACHE
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.clockcache.enable", "true"));
	private static final boolean PARANOID_CHECKS
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.checks.paranoid", "true"));
	private static final boolean VERIFY_COMPRESSION
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.checks.compression", "false"));
	private static final boolean VERIFY_FILE_SIZE
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.checks.filesize", "false"));
	private static final boolean PARANOID_FILE_CHECKS
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.checks.paranoidfilechecks", "false"));
	private static final boolean FORCE_COLUMN_FAMILY_CONSISTENCY_CHECKS
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.checks.forcecolumnfamilyconsistencychecks", "true"));
	private static final InfoLogLevel LOG_LEVEL = InfoLogLevel.getInfoLogLevel(Byte.parseByte(System.getProperty("it.cavallium.dbengine.log.levelcode", "" + InfoLogLevel.WARN_LEVEL.getValue())));

	private static final CacheFactory CACHE_FACTORY = USE_CLOCK_CACHE ? new HyperClockCacheFactory() : new LRUCacheFactory();
	private static final boolean ALLOW_SNAPSHOTS = Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.snapshots.allow", "true"));

	static {
		RocksDB.loadLibrary();
	}

	protected static final Logger logger = LogManager.getLogger(LLLocalKeyValueDatabase.class);

	private final MeterRegistry meterRegistry;
	private ForkJoinPool dbReadPool;
	private ForkJoinPool dbWritePool;

	private final Timer snapshotTime;

	// Configurations

	private final Path dbPath;
	private final String name;
	private final DatabaseOptions databaseOptions;

	private final boolean enableColumnsBug;
	private final RocksDBRefs refs = new RocksDBRefs();
	private RocksDB db;
	private Statistics statistics;
	private Cache standardCache;
	private final Map<Column, ColumnFamilyHandle> handles;

	private final HashMap<String, PersistentCache> persistentCaches;
	private final ConcurrentHashMap<Long, Snapshot> snapshotsHandles = new ConcurrentHashMap<>();
	private final AtomicLong nextSnapshotNumbers = new AtomicLong(1);
	private final StampedLock closeLock = new StampedLock();
	private volatile boolean closeRequested = false;
	private volatile boolean closed = false;

	@SuppressWarnings("SwitchStatementWithTooFewBranches")
	public LLLocalKeyValueDatabase(@NotNull MeterRegistry meterRegistry,
			String name,
			boolean inMemory,
			@Nullable Path path,
			List<Column> columns,
			List<ColumnFamilyHandle> handles,
			DatabaseOptions databaseOptions) {
		this.name = name;
		this.meterRegistry = meterRegistry;
		this.dbReadPool = StreamUtils.newNamedForkJoinPool("db-" + name, false);
		this.dbWritePool = StreamUtils.newNamedForkJoinPool("db-" + name, false);

		this.snapshotTime = Timer
				.builder("db.snapshot.timer")
				.publishPercentiles(0.2, 0.5, 0.95)
				.publishPercentileHistogram()
				.tags("db.name", name)
				.register(meterRegistry);

		this.enableColumnsBug = "true".equals(databaseOptions.extraFlags().getOrDefault("enableColumnBug", "false"));

		if (!enableColumnsBug) {
			if (columns.stream().noneMatch(column -> column.name().equals("default"))) {
				columns = Stream.concat(Stream.of(Column.of("default")), columns.stream()).toList();
			}
		}

		OptionsWithCache optionsWithCache = openRocksDb(path, databaseOptions, refs);
		var rocksdbOptions = optionsWithCache.options();
		try {
			List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();

			var defaultColumnOptions = new ColumnFamilyOptions();
			refs.track(defaultColumnOptions);
			descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultColumnOptions));

			// Check column names validity
			for (NamedColumnOptions columnOption : databaseOptions.columnOptions()) {
				if (columns.stream().map(Column::name).noneMatch(columnName -> columnName.equals(columnOption.name()))) {
					throw new IllegalArgumentException(
							"Column " + columnOption.name() + " does not exist. Available columns: " + columns
									.stream()
									.map(Column::name)
									.collect(Collectors.joining(", ", "[", "]")));
				}
			}

			var rocksLogger = new RocksLog4jLogger(rocksdbOptions, logger);
			this.persistentCaches = new HashMap<>();

			for (Column column : columns) {
				var columnFamilyOptions = new ColumnFamilyOptions();
				refs.track(columnFamilyOptions);

				columnFamilyOptions
						.setForceConsistencyChecks(FORCE_COLUMN_FAMILY_CONSISTENCY_CHECKS)
						.setParanoidFileChecks(PARANOID_FILE_CHECKS);

				var columnOptions = databaseOptions
						.columnOptions()
						.stream()
						.filter(opts -> opts.name().equals(column.name()))
						.findFirst()
						.map(NamedColumnOptions::options)
						.orElseGet(databaseOptions::defaultColumnOptions);

				//noinspection ConstantConditions
				if (columnOptions.memtableMemoryBudgetBytes() != null) {
					// about 512MB of ram will be used for level style compaction
					columnFamilyOptions.optimizeLevelStyleCompaction(columnOptions.memtableMemoryBudgetBytes().orElse(
							databaseOptions.lowMemory()
									? (DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET / 4)
									: DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET));
				}

				if (isDisableAutoCompactions()) {
					columnFamilyOptions.setDisableAutoCompactions(true);
				}
				var blobFiles = columnOptions.blobFiles();
				columnFamilyOptions.setEnableBlobFiles(blobFiles);
				try {
					columnFamilyOptions.setPrepopulateBlobCache(PrepopulateBlobCache.PREPOPULATE_BLOB_FLUSH_ONLY);
				} catch (Throwable ex) {
					logger.error("Failed to set prepopulate blob cache", ex);
				}
				if (blobFiles) {
					if (columnOptions.blobFileSize().isPresent()) {
						columnFamilyOptions.setBlobFileSize(columnOptions.blobFileSize().get());
					}
					if (columnOptions.minBlobSize().isPresent()) {
						columnFamilyOptions.setMinBlobSize(columnOptions.minBlobSize().get());
					}
					if (columnOptions.blobCompressionType().isPresent()) {
						columnFamilyOptions.setCompressionType(columnOptions.blobCompressionType().get().getType());
					} else {
						columnFamilyOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
					}
					columnFamilyOptions.setBlobCompactionReadaheadSize(4 * SizeUnit.MB);
					columnFamilyOptions.setEnableBlobGarbageCollection(true);
				}

				// This option is not supported with multiple db paths
				// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
				// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
				boolean dynamicLevelBytes = databaseOptions.volumes().size() <= 1;
				if (dynamicLevelBytes) {
					columnFamilyOptions.setLevelCompactionDynamicLevelBytes(true);
				} else {
					columnFamilyOptions.setLevelCompactionDynamicLevelBytes(false);
					// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
					// https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
					columnFamilyOptions.setMaxBytesForLevelBase(256 * SizeUnit.MB);
					// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
					columnFamilyOptions.setMaxBytesForLevelMultiplier(10);
				}
				if (isDisableAutoCompactions()) {
					columnFamilyOptions.setLevel0FileNumCompactionTrigger(-1);
				} else if (!FOLLOW_ROCKSDB_OPTIMIZATIONS) {
					// ArangoDB uses a value of 2: https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
					// Higher values speed up writes, but slow down reads
					columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);
				}
				if (isDisableSlowdown()) {
					columnFamilyOptions.setLevel0SlowdownWritesTrigger(-1);
					columnFamilyOptions.setLevel0StopWritesTrigger(Integer.MAX_VALUE);
					columnFamilyOptions.setHardPendingCompactionBytesLimit(Long.MAX_VALUE);
					columnFamilyOptions.setSoftPendingCompactionBytesLimit(Long.MAX_VALUE);
				} {
					// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
					columnFamilyOptions.setLevel0SlowdownWritesTrigger(20);
					// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
					columnFamilyOptions.setLevel0StopWritesTrigger(36);
				}

				if (!columnOptions.levels().isEmpty()) {
					columnFamilyOptions.setNumLevels(columnOptions.levels().size());
					var firstLevelOptions = getRocksLevelOptions(columnOptions.levels().get(0), refs);
					columnFamilyOptions.setCompressionType(firstLevelOptions.compressionType);
					columnFamilyOptions.setCompressionOptions(firstLevelOptions.compressionOptions);

					var lastLevelOptions = getRocksLevelOptions(columnOptions
							.levels()
							.get(columnOptions.levels().size() - 1), refs);
					columnFamilyOptions.setBottommostCompressionType(lastLevelOptions.compressionType);
					columnFamilyOptions.setBottommostCompressionOptions(lastLevelOptions.compressionOptions);

					columnFamilyOptions.setCompressionPerLevel(mapList(columnOptions.levels(), v -> v.compression().getType()));
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
					columnFamilyOptions.setBottommostCompressionType(CompressionType.LZ4HC_COMPRESSION);
					var compressionOptions = new CompressionOptions()
							.setEnabled(true)
							.setMaxDictBytes(32768);
					refs.track(compressionOptions);
					columnFamilyOptions.setBottommostCompressionOptions(compressionOptions);
					columnFamilyOptions.setCompressionPerLevel(compressionTypes);
				}

				final TableFormatConfig tableOptions = inMemory ? new PlainTableConfig() : new BlockBasedTableConfig();
				if (!FOLLOW_ROCKSDB_OPTIMIZATIONS) {
					if (!databaseOptions.lowMemory()) {
						// tableOptions.setOptimizeFiltersForMemory(true);
						columnFamilyOptions.setWriteBufferSize(256 * SizeUnit.MB);
					}
				}
				if (columnOptions.writeBufferSize().isPresent()) {
					columnFamilyOptions.setWriteBufferSize(columnOptions.writeBufferSize().get());
				}
				columnFamilyOptions.setMaxWriteBufferNumberToMaintain(1);
				if (tableOptions instanceof BlockBasedTableConfig blockBasedTableConfig) {
					blockBasedTableConfig.setVerifyCompression(VERIFY_COMPRESSION);
				}
				if (columnOptions.filter().isPresent()) {
					var filterOptions = columnOptions.filter().get();

					if (filterOptions instanceof it.cavallium.dbengine.rpc.current.data.BloomFilter bloomFilterOptions) {
						// If OptimizeFiltersForHits == true: memory size = bitsPerKey * (totalKeys * 0.1)
						// If OptimizeFiltersForHits == false: memory size = bitsPerKey * totalKeys
						final BloomFilter bloomFilter = new BloomFilter(bloomFilterOptions.bitsPerKey());
						refs.track(bloomFilter);
						if (tableOptions instanceof BlockBasedTableConfig blockBasedTableConfig) {
							blockBasedTableConfig.setFilterPolicy(bloomFilter);
						}
					} else if (filterOptions instanceof NoFilter) {
						if (tableOptions instanceof BlockBasedTableConfig blockBasedTableConfig) {
							blockBasedTableConfig.setFilterPolicy(null);
						}
					}
				}
				boolean cacheIndexAndFilterBlocks = columnOptions.cacheIndexAndFilterBlocks()
						// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
						.orElse(true);
				if (databaseOptions.spinning()) {
					// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
					// cacheIndexAndFilterBlocks = true;
					// https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
					columnFamilyOptions.setMinWriteBufferNumberToMerge(3);
					// https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
					columnFamilyOptions.setMaxWriteBufferNumber(4);
				}
				if (tableOptions instanceof BlockBasedTableConfig blockBasedTableConfig) {
					blockBasedTableConfig
							// http://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
							.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash)
							// http://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
							.setDataBlockHashTableUtilRatio(0.75)
							// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
							.setPinTopLevelIndexAndFilter(true)
							// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
							.setPinL0FilterAndIndexBlocksInCache(true)
							// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
							.setCacheIndexAndFilterBlocksWithHighPriority(true)
							.setCacheIndexAndFilterBlocks(cacheIndexAndFilterBlocks)
							// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
							// Enabling partition filters increase the reads by 2x
							.setPartitionFilters(columnOptions.partitionFilters().orElse(false))
							// https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
							.setIndexType(columnOptions.partitionFilters().orElse(false) ? IndexType.kTwoLevelIndexSearch : IndexType.kBinarySearch)
							.setChecksumType(ChecksumType.kXXH3)
							.setVerifyCompression(VERIFY_COMPRESSION)
							// Spinning disks: 64KiB to 256KiB (also 512KiB). SSDs: 16KiB
							// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
							// https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
							.setBlockSize(columnOptions.blockSize().orElse((databaseOptions.spinning() ? 128 : 16) * 1024))
							.setBlockCache(optionsWithCache.standardCache())
							.setPersistentCache(resolvePersistentCache(persistentCaches,
									rocksdbOptions,
									databaseOptions.persistentCaches(),
									columnOptions.persistentCacheId(),
									refs,
									rocksLogger
							));
				}

				columnFamilyOptions.setTableFormatConfig(tableOptions);
				if (inMemory) {
					columnFamilyOptions.useFixedLengthPrefixExtractor(3);
				}
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

				if (!FOLLOW_ROCKSDB_OPTIMIZATIONS) {
					// // Increasing this value can reduce the frequency of compaction and reduce write amplification,
					// // but it will also cause old data to be unable to be cleaned up in time, thus increasing read amplification.
					// // This parameter is not easy to adjust. It is generally not recommended to set it above 256MB.
					// https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
					columnFamilyOptions.setTargetFileSizeBase(64 * SizeUnit.MB);
					// // For each level up, the threshold is multiplied by the factor target_file_size_multiplier
					// // (but the default value is 1, which means that the maximum sstable of each level is the same).
					columnFamilyOptions.setTargetFileSizeMultiplier(2);
				}

				descriptors.add(new ColumnFamilyDescriptor(column.name().getBytes(StandardCharsets.US_ASCII), columnFamilyOptions));
			}

			// Get databases directory path
			requireNonNull(path);
			Path databasesDirPath = path.toAbsolutePath().getParent();
			String dbPathString = databasesDirPath.toString() + File.separatorChar + path.getFileName();
			this.dbPath = Paths.get(dbPathString);

			// Set options
			this.databaseOptions = databaseOptions;

			var statsLevel = System.getProperty("it.cavallium.dbengine.stats.level");
			if (statsLevel != null) {
				this.statistics = registerStatistics(name, rocksdbOptions, meterRegistry, StatsLevel.valueOf(statsLevel));
			} else {
				this.statistics = null;
			}

			while (true) {
				try {
					// a factory method that returns a RocksDB instance
					if (databaseOptions.openAsSecondary()) {
						var secondaryPath = dbPath
								.resolve("secondary-log")
								.resolve(databaseOptions.secondaryDirectoryName().orElse("unnamed-" + UUID.randomUUID()));
						try {
							Files.createDirectories(secondaryPath);
						} catch (IOException e) {
							throw new RocksDBException("Failed to create secondary exception: " + e);
						}
						this.db = RocksDB.openReadOnly(rocksdbOptions,
								dbPathString,
								descriptors,
								handles,
								false
						);
					} else if (databaseOptions.optimistic()) {
						this.db = OptimisticTransactionDB.open(rocksdbOptions, dbPathString, descriptors, handles);
					} else {
						var transactionOptions = new TransactionDBOptions()
							.setWritePolicy(TxnDBWritePolicy.WRITE_COMMITTED)
							.setTransactionLockTimeout(5000)
							.setDefaultLockTimeout(5000);
						refs.track(transactionOptions);
						this.db = TransactionDB.open(rocksdbOptions,
								transactionOptions,
								dbPathString,
								descriptors,
								handles
						);
					}
					this.standardCache = optionsWithCache.standardCache;
					break;
				} catch (RocksDBException ex) {
					switch (ex.getMessage()) {
						case "Direct I/O is not supported by the specified DB." -> {
							logger.warn("RocksDB options failed: {}", ex.getLocalizedMessage());
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

			handles.forEach(refs::track);

			// compactDb(db, handles);
			if (!databaseOptions.openAsSecondary()) {
				logger.info("Flushing database at {}", dbPathString);
				flushDb(db, handles);
			}
		} catch (RocksDBException ex) {
			throw new DBException(ex);
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

		for (RocksDBLongProperty property : RocksDBLongProperty.values()) {
			registerGauge(meterRegistry, name, property.getName(), property.isDividedByColumnFamily());
		}
		// Bloom seek stats
		registerGauge(meterRegistry, name, "rocksdb.bloom.filter.prefix.useful", true);
		registerGauge(meterRegistry, name, "rocksdb.bloom.filter.prefix.checked", true);
		// Bloom point lookup stats
		registerGauge(meterRegistry, name, "rocksdb.bloom.filter.useful", true);
		registerGauge(meterRegistry, name, "rocksdb.bloom.filter.full.positive", true);
		registerGauge(meterRegistry, name, "rocksdb.bloom.filter.full.true.positive", true);
	}

	public static boolean isDisableAutoCompactions() {
		return parseBoolean(System.getProperty("it.cavallium.dbengine.compactions.auto.disable", "false"));
	}

	public static boolean isDisableSlowdown() {
		return isDisableAutoCompactions()
				|| parseBoolean(System.getProperty("it.cavallium.dbengine.disableslowdown", "false"));
	}

	protected void ensureOpen() {
		if (closed) {
			throw new IllegalStateException("Database closed");
		}
		RocksDBUtils.ensureOpen(db, null);
	}

	protected void ensureOwned(AbstractImmutableNativeReference rocksObject) {
		RocksDBUtils.ensureOwned(rocksObject);
	}

	private synchronized PersistentCache resolvePersistentCache(HashMap<String, PersistentCache> caches,
			DBOptions rocksdbOptions,
			List<it.cavallium.dbengine.rpc.current.data.PersistentCache> persistentCaches,
			NullableString persistentCacheId,
			RocksDBRefs refs,
			RocksLog4jLogger rocksLogger) throws RocksDBException {
		if (persistentCacheId.isEmpty()) {
			return null;
		}
		var existingPersistentCache = caches.get(persistentCacheId.get());
		if (existingPersistentCache != null) {
			return existingPersistentCache;
		}

		var foundCaches = persistentCaches
				.stream()
				.filter(cache -> cache.id().equals(persistentCacheId.get()))
				.toList();
		if (foundCaches.size() > 1) {
			throw new IllegalArgumentException("There are " + foundCaches.size()
					+ " defined persistent caches with the id \"" + persistentCacheId.get() + "\"");
		}
		for (it.cavallium.dbengine.rpc.current.data.PersistentCache foundCache : foundCaches) {
			var persistentCache = new PersistentCache(Env.getDefault(),
					foundCache.path(),
					foundCache.size(),
					rocksLogger,
					foundCache.optimizeForNvm()
			);
			refs.track(persistentCache);
			var prev = caches.put(persistentCacheId.get(), persistentCache);
			if (prev != null) {
				throw new IllegalStateException();
			}
			return persistentCache;
		}
		throw new IllegalArgumentException("Persistent cache " + persistentCacheId.get() + " is not defined");
	}

	public Map<Column, ColumnFamilyHandle> getAllColumnFamilyHandles() {
		return this.handles;
	}

	public int getLastVolumeId() {
		var paths = convertPaths(dbPath.toAbsolutePath().getParent(), dbPath.getFileName(), databaseOptions.volumes());
		return paths.size() - 1;
	}

	public int getLevels(Column column) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			var cfh = handles.get(column);
			ensureOwned(cfh);
			return RocksDBUtils.getLevels(db, cfh);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	public Stream<RocksDBFile> getAllLiveFiles() throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			db.getLiveFiles(); // flushes the memtable
			var liveFilesMetadata = db.getLiveFilesMetaData();
			List<RocksDBFile> files = new ArrayList<>();
			for (LiveFileMetaData file : liveFilesMetadata) {
				files.add(new RocksDBColumnFile(db, getCfh(file.columnFamilyName()), file));
			}
			return files.stream();
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	public List<RocksDBFile> getColumnFiles(Column column, boolean excludeLastLevel) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			var cfh = handles.get(column);
			ensureOwned(cfh);
			return RocksDBUtils.getColumnFiles(db, cfh, excludeLastLevel);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	public void forceCompaction(int volumeId) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			for (var cfh : this.handles.values()) {
				ensureOwned(cfh);
				RocksDBUtils.forceCompaction(db, name, cfh, volumeId, logger);
			}
		} catch (RocksDBException e) {
			throw new DBException("Failed to force compaction", e);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	public void flush(FlushOptions flushOptions) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(flushOptions);
			db.flush(flushOptions, List.copyOf(getAllColumnFamilyHandles().values()));
			db.flushWal(true);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void preClose() {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			try (var fo = new FlushOptions().setWaitForFlush(true)) {
				flush(fo);
			} catch (RocksDBException ex) {
				throw new DBException(ex);
			}
			db.cancelAllBackgroundWork(true);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	protected void onPauseForBackup() {
		pauseWrites();
	}

	@Override
	protected void onResumeAfterBackup() {
		resumeWrites();
	}

	@Override
	public void ingestSST(Column column, Stream<Path> files, boolean replaceExisting) {
		var columnHandle = handles.get(column);
		if (columnHandle == null) {
			logger.warn("Column {} doesn't exist", column);
			return;
		}
		collect(files, iterating(sst -> {
			try (var opts = new IngestExternalFileOptions()) {
				opts.setIngestBehind(!replaceExisting);
				opts.setSnapshotConsistency(false);
				opts.setAllowBlockingFlush(true);
				opts.setMoveFiles(true);
				db.ingestExternalFile(columnHandle, List.of(sst.toString()), opts);
			} catch (RocksDBException ex) {
				throw new DBException(new DBException("Failed to ingest SST file " + sst, ex));
			}
		}));
	}

	private record RocksLevelOptions(CompressionType compressionType, CompressionOptions compressionOptions) {}
	private RocksLevelOptions getRocksLevelOptions(DatabaseLevel levelOptions, RocksDBRefs refs) {
		var compressionType = levelOptions.compression().getType();
		var compressionOptions = new CompressionOptions();
		refs.track(compressionOptions);
		if (compressionType != CompressionType.NO_COMPRESSION) {
			compressionOptions.setEnabled(true);
			compressionOptions.setMaxDictBytes(levelOptions.maxDictBytes());
		} else {
			compressionOptions.setEnabled(false);
		}
		return new RocksLevelOptions(compressionType, compressionOptions);
	}

	private void registerGauge(MeterRegistry meterRegistry, String name, String propertyName, boolean divideByAllColumns) {
		if (divideByAllColumns) {
			for (var cfhEntry : handles.entrySet()) {
				var columnName = cfhEntry.getKey().name();
				var cfh = cfhEntry.getValue();
				meterRegistry.gauge("rocksdb.property.value",
						List.of(Tag.of("db.name", name), Tag.of("db.column.name", columnName), Tag.of("db.property.name", propertyName)),
						db,
						database -> {
							if (closed) {
								return 0d;
							}
							var closeReadLock = closeLock.readLock();
							try {
								if (closed) {
									return 0d;
								}
								return database.getLongProperty(cfh, propertyName);
							} catch (RocksDBException e) {
								if ("NotFound".equals(e.getMessage())) {
									return 0d;
								}
								throw new RuntimeException(e);
							} finally {
								closeLock.unlockRead(closeReadLock);
							}
						}
				);
			}
		} else {
			meterRegistry.gauge("rocksdb.property.value",
					List.of(Tag.of("db.name", name), Tag.of("db.property.name", propertyName)),
					db,
					database -> {
						if (closed) {
							return 0d;
						}
						var closeReadLock = closeLock.readLock();
						try {
							if (closed) {
								return 0d;
							}
							return database.getAggregatedLongProperty(propertyName) / (double) handles.size();
						} catch (RocksDBException e) {
							if ("NotFound".equals(e.getMessage())) {
								return 0d;
							}
							throw new RuntimeException(e);
						} finally {
							closeLock.unlockRead(closeReadLock);
						}
					}
			);
		}
	}

	@Override
	public String getDatabaseName() {
		return name;
	}

	@Override
	public ForkJoinPool getDbReadPool() {
		return dbReadPool;
	}

	@Override
	public ForkJoinPool getDbWritePool() {
		return dbWritePool;
	}

	public StampedLock getCloseLock() {
		return closeLock;
	}

	private void flushAndCloseDb(RocksDB db, Cache standardCache, List<ColumnFamilyHandle> handles) {
		var closeWriteLock = closeLock.writeLock();
		try {
			if (closed) {
				return;
			}
			closed = true;
			if (db.isOwningHandle()) {
				//flushDb(db, handles);
			}

			snapshotsHandles.forEach((id, snapshot) -> {
				try {
					if (db.isOwningHandle()) {
						db.releaseSnapshot(snapshot);
						snapshot.close();
					}
				} catch (Exception ex2) {
					// ignore exception
					logger.debug("Failed to release snapshot " + id, ex2);
				}
			});
			snapshotsHandles.clear();
			try {
				db.closeE();
			} catch (Exception ex) {
				logger.error("Can't close database " + name + " at " + dbPath, ex);
			}
			for (ColumnFamilyHandle handle : handles) {
				try {
					handle.close();
				} catch (Exception ex) {
					logger.error("Can't close column family", ex);
				}
			}
			if (standardCache != null) {
				standardCache.close();
			}

			for (PersistentCache persistentCache : persistentCaches.values()) {
				try {
					persistentCache.close();
				} catch (Exception ex) {
					logger.error("Can't close persistent cache", ex);
				}
			}
			refs.close();
		} finally {
			closeLock.unlockWrite(closeWriteLock);
		}
	}

	private void flushDb(RocksDB db, List<ColumnFamilyHandle> handles) throws RocksDBException {
		if (LLUtils.isInNonBlockingThread()) {
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
		if (LLUtils.isInNonBlockingThread()) {
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
					try (var cro = new CompactRangeOptions()
							.setAllowWriteStall(true)
							.setExclusiveManualCompaction(true)
							.setChangeLevel(false)) {
						db.compactRange(cfh, null, null, cro);
					}
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


	record OptionsWithCache(DBOptions options, @Nullable Cache standardCache) {
	}

	private static OptionsWithCache openRocksDb(@Nullable Path path, DatabaseOptions databaseOptions, RocksDBRefs refs) {
		try {
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
			//noinspection ConstantConditions
			if (databaseOptions.persistentCaches() != null) {
				for (var persistentCache : databaseOptions.persistentCaches()) {
					var persistentCachePath = Paths.get(persistentCache.path());
					if (Files.notExists(persistentCachePath)) {
						Files.createDirectories(persistentCachePath);
						if (!Files.isDirectory(persistentCachePath)) {
							throw new IllegalArgumentException(
									"Persistent cache \"" + persistentCache.id() + "\" path \"" + persistentCachePath
											+ "\" is not a directory!");
						}
					}
				}
			}

			// the Options class contains a set of configurable DB options
			// that determines the behaviour of the database.
			var options = new DBOptions();
			refs.track(options);
			options.setParanoidChecks(PARANOID_CHECKS);
			options.setSkipCheckingSstFileSizesOnDbOpen(!VERIFY_FILE_SIZE);
			options.setEnablePipelinedWrite(true);
			var maxSubCompactions = Integer.parseInt(System.getProperty("it.cavallium.dbengine.compactions.max.sub", "-1"));
			if (maxSubCompactions > 0) {
				options.setMaxSubcompactions(maxSubCompactions);
			}
			var customWriteRate = Long.parseLong(System.getProperty("it.cavallium.dbengine.write.delayedrate", "-1"));
			if (customWriteRate >= 0) {
				options.setDelayedWriteRate(customWriteRate);
			}
			if (databaseOptions.logPath().isPresent()) {
				options.setDbLogDir(databaseOptions.logPath().get());
			}
			if (databaseOptions.walPath().isPresent()) {
				options.setWalDir(databaseOptions.walPath().get());
			}
			options.setCreateIfMissing(true);
			options.setSkipStatsUpdateOnDbOpen(true);
			options.setCreateMissingColumnFamilies(true);
			options.setInfoLogLevel(LOG_LEVEL);
			// todo: automatically flush every x seconds?

			options.setManualWalFlush(true);

			options.setAvoidFlushDuringShutdown(false); // Flush all WALs during shutdown
			options.setAvoidFlushDuringRecovery(true); // Flush all WALs during startup
			options.setWalRecoveryMode(databaseOptions.absoluteConsistency()
					? WALRecoveryMode.AbsoluteConsistency
					: WALRecoveryMode.PointInTimeRecovery); // Crash if the WALs are corrupted.Default: TolerateCorruptedTailRecords
			options.setDeleteObsoleteFilesPeriodMicros(20 * 1000000); // 20 seconds
			options.setKeepLogFileNum(10);

			requireNonNull(databasesDirPath);
			requireNonNull(path.getFileName());
			List<DbPath> paths = mapList(convertPaths(databasesDirPath, path.getFileName(), databaseOptions.volumes()),
					p -> new DbPath(p.path, p.targetSize)
			);
			options.setDbPaths(paths);
			options.setMaxOpenFiles(databaseOptions.maxOpenFiles().orElse(-1));
			options.setMaxFileOpeningThreads(Runtime.getRuntime().availableProcessors());
			if (databaseOptions.spinning()) {
				// https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
				options.setUseFsync(false);
			}

			long writeBufferManagerSize;
			if (databaseOptions.writeBufferManager().isPresent()) {
				writeBufferManagerSize = databaseOptions.writeBufferManager().get();
			} else {
				writeBufferManagerSize = 0;
			}

			if (isDisableAutoCompactions()) {
				options.setMaxBackgroundCompactions(0);
				options.setMaxBackgroundJobs(0);
			} else {
				var backgroundJobs = Integer.parseInt(System.getProperty("it.cavallium.dbengine.jobs.background.num", "-1"));
				if (backgroundJobs >= 0) {
					options.setMaxBackgroundJobs(backgroundJobs);
				} else if (databaseOptions.spinning()) {
					// https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
					options.setMaxBackgroundJobs(4);
				}
			}

			Cache blockCache;
			final boolean useDirectIO = databaseOptions.useDirectIO();
			final boolean allowMmapReads = !useDirectIO && databaseOptions.allowMemoryMapping();
			final boolean allowMmapWrites = !useDirectIO && (databaseOptions.allowMemoryMapping()
					|| parseBoolean(System.getProperty("it.cavallium.dbengine.mmapwrites.enable", "false")));

			// todo: replace with a real option called database-write-buffer-size
			// 0 = default = disabled
			long dbWriteBufferSize = Long.parseLong(System.getProperty("it.cavallium.dbengine.dbwritebuffer.size", "0"));

			if (databaseOptions.lowMemory()) {
				// LOW MEMORY
				options
						.setBytesPerSync(0) // default
						.setWalBytesPerSync(0) // default
						.setIncreaseParallelism(1)
						.setDbWriteBufferSize(Math.min(dbWriteBufferSize, 8 * SizeUnit.MB))
						.setWalTtlSeconds(60)
						.setMaxTotalWalSize(10 * SizeUnit.GB)
				;
				blockCache = CACHE_FACTORY.newCache(writeBufferManagerSize + databaseOptions.blockCache().orElse(8L * SizeUnit.MB));
				refs.track(blockCache);

				if (useDirectIO) {
					options
							// Option to enable readahead in compaction
							// If not set, it will be set to 2MB internally
							.setCompactionReadaheadSize(2 * SizeUnit.MB) // recommend at least 2MB
							// Option to tune write buffer for direct writes
							.setWritableFileMaxBufferSize(SizeUnit.MB)
					;
				}
				if (databaseOptions.spinning()) {
					options
							// method documentation
							.setCompactionReadaheadSize(4 * SizeUnit.MB)
							// guessed
							.setWritableFileMaxBufferSize(2 * SizeUnit.MB);
				}
			} else {
				// HIGH MEMORY
				options
						.setDbWriteBufferSize(dbWriteBufferSize)
						.setBytesPerSync(64 * SizeUnit.MB)
						.setWalBytesPerSync(64 * SizeUnit.MB)

						.setWalTtlSeconds(80) // Auto
						.setWalSizeLimitMB(0) // Auto
						.setMaxTotalWalSize(0) // AUto
				;
				blockCache = CACHE_FACTORY.newCache(writeBufferManagerSize + databaseOptions.blockCache().orElse( 512 * SizeUnit.MB));
				refs.track(blockCache);

				if (useDirectIO) {
					options
							// Option to enable readahead in compaction
							// If not set, it will be set to 2MB internally
							.setCompactionReadaheadSize(4 * SizeUnit.MB) // recommend at least 2MB
							// Option to tune write buffer for direct writes
							.setWritableFileMaxBufferSize(2 * SizeUnit.MB)
					;
				}
				if (databaseOptions.spinning()) {
					options
							// method documentation
							.setCompactionReadaheadSize(16 * SizeUnit.MB)
							// guessed
							.setWritableFileMaxBufferSize(8 * SizeUnit.MB);
				}
				options.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
			}

			if (databaseOptions.writeBufferManager().isPresent()) {
				var writeBufferManager = new WriteBufferManager(writeBufferManagerSize, blockCache, false);
				refs.track(writeBufferManager);
				options.setWriteBufferManager(writeBufferManager);
			}

			if (useDirectIO) {
				options
						.setAllowMmapReads(false)
						.setAllowMmapWrites(false)
						.setUseDirectReads(true)
				;
			} else {
				options
						.setAllowMmapReads(allowMmapReads)
						.setAllowMmapWrites(allowMmapWrites);
			}

			if (useDirectIO || !allowMmapWrites) {
				options.setUseDirectIoForFlushAndCompaction(true);
			}

			return new OptionsWithCache(options, blockCache);
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	record DbPathRecord(Path path, long targetSize) {}

	private static List<DbPathRecord> convertPaths(Path databasesDirPath, Path path, List<DatabaseVolume> volumes) {
		var paths = new ArrayList<DbPathRecord>(volumes.size());
		if (volumes.isEmpty()) {
			return List.of(new DbPathRecord(databasesDirPath.resolve(path.getFileName() + "_hot"),
							0), // Legacy
					new DbPathRecord(databasesDirPath.resolve(path.getFileName() + "_cold"),
							0), // Legacy
					new DbPathRecord(databasesDirPath.resolve(path.getFileName() + "_colder"),
							1000L * 1024L * 1024L * 1024L)  // 1000GiB
			); // Legacy
		}
		for (DatabaseVolume volume : volumes) {
			Path volumePath;
			if (volume.volumePath().isAbsolute()) {
				volumePath = volume.volumePath();
			} else {
				volumePath = databasesDirPath.resolve(volume.volumePath());
			}
			paths.add(new DbPathRecord(volumePath, volume.targetSizeBytes()));
		}
		return paths;
	}

	private Statistics registerStatistics(String dbName, DBOptions dbOptions, MeterRegistry meterRegistry,
			StatsLevel statsLevel) {
			Statistics stats = new Statistics();
			stats.setStatsLevel(statsLevel);
			dbOptions.setStatistics(stats);
		for (TickerType tickerType : TickerType.values()) {
			if (tickerType == TickerType.TICKER_ENUM_MAX) {
				continue;
			}
			meterRegistry.gauge("rocksdb.statistics.value",
					List.of(Tag.of("db.name", dbName), Tag.of("db.statistics.name", tickerType.name())),
					stats,
					statistics -> {
						if (closeRequested || closed) return 0d;
						long closeReadLock = 0;
						try {
							closeReadLock = closeLock.tryReadLock(1, TimeUnit.SECONDS);
						} catch (InterruptedException ignored) {}
						try {
							if (closeRequested || closed || closeReadLock == 0) return 0d;
							return statistics.getTickerCount(tickerType);
						} finally {
							closeLock.unlockRead(closeReadLock);
						}
					}
			);
		}
		return stats;
	}

	private Snapshot getSnapshotLambda(LLSnapshot snapshot) {
		var closeReadSnapLock = closeLock.readLock();
		try {
			ensureOpen();
			var snapshotHandle = snapshotsHandles.get(snapshot.getSequenceNumber());
			//ensureOwned(snapshotHandle);
			return snapshotHandle;
		} finally {
			closeLock.unlockRead(closeReadSnapLock);
		}
	}

	@Override
	public LLLocalSingleton getSingleton(byte[] singletonListColumnName,
			byte[] name,
			byte @Nullable[] defaultValue) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			var cfh = getCfh(singletonListColumnName);
			ensureOwned(cfh);
			return new LLLocalSingleton(getRocksDBColumn(db, cfh),
					this::getSnapshotLambda,
					LLLocalKeyValueDatabase.this.name,
					name,
					ColumnUtils.toString(singletonListColumnName),
					defaultValue
			);
		} catch (RocksDBException ex) {
			throw new DBException("Failed to read " + Arrays.toString(name), ex);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public LLLocalDictionary getDictionary(byte[] columnName, UpdateMode updateMode) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			var cfh = getCfh(columnName);
			ensureOwned(cfh);
			return new LLLocalDictionary(getRocksDBColumn(db, cfh),
					name,
					ColumnUtils.toString(columnName),
					this::getSnapshotLambda,
					updateMode
			);
		} catch (RocksDBException e) {
			throw new DBException(e);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	public RocksDBColumn getRocksDBColumn(byte[] columnName) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ColumnFamilyHandle cfh;
			try {
				cfh = getCfh(columnName);
				ensureOwned(cfh);
			} catch (RocksDBException e) {
				throw new UnsupportedOperationException("Column family doesn't exist: " + Arrays.toString(columnName), e);
			}
			return getRocksDBColumn(db, cfh);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	private RocksDBColumn getRocksDBColumn(RocksDB db, ColumnFamilyHandle cfh) {
		var closeLock = getCloseLock();
		if (db instanceof OptimisticTransactionDB optimisticTransactionDB) {
			return new OptimisticRocksDBColumn(optimisticTransactionDB,
					name,
					cfh,
					meterRegistry,
					closeLock,
					dbReadPool,
					dbWritePool
			);
		} else if (db instanceof TransactionDB transactionDB) {
			return new PessimisticRocksDBColumn(transactionDB,
					name,
					cfh,
					meterRegistry,
					closeLock,
					dbReadPool,
					dbWritePool
			);
		} else {
			return new StandardRocksDBColumn(db, name, cfh, meterRegistry, closeLock, dbReadPool, dbWritePool);
		}
	}

	private ColumnFamilyHandle getCfh(byte[] columnName) throws RocksDBException {
		var cfh = handles.get(ColumnUtils.special(ColumnUtils.toString(columnName)));
		assert enableColumnsBug || Arrays.equals(cfh.getName(), columnName);
		return cfh;
	}

	public DatabaseOptions getDatabaseOptions() {
		return databaseOptions;
	}

	public Stream<Path> getSSTS() {
		var paths = convertPaths(dbPath.toAbsolutePath().getParent(), dbPath.getFileName(), databaseOptions.volumes());

		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			return db.getLiveFiles().files.stream()
					.filter(file -> file.endsWith(".sst"))
					.map(file -> file.substring(1))
					.mapMulti((file, sink) -> {
						{
							var path = dbPath.resolve(file);
							if (Files.exists(path)) {
								sink.accept(path);
								return;
							}
						}
						for (var volumePath : paths) {
							var path = volumePath.path().resolve(file);
							if (Files.exists(path)) {
								sink.accept(path);
								return;
							}
						}
					});
		} catch (RocksDBException e) {
			throw new DBException(e);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	public void ingestSSTS(Stream<Path> sstsFlux) {
		collect(sstsFlux.map(path -> path.toAbsolutePath().toString()), iterating(sst -> {
			var closeReadLock = closeLock.readLock();
			try (var opts = new IngestExternalFileOptions()) {
				try {
					logger.info("Ingesting SST \"{}\"...", sst);
					db.ingestExternalFile(List.of(sst), opts);
					logger.info("Ingested SST \"{}\" successfully", sst);
				} catch (RocksDBException e) {
					logger.error("Can't ingest SST \"{}\"", sst, e);
				}
			} finally {
				closeLock.unlockRead(closeReadLock);
			}
		}));
	}

	@Override
	public MemoryStats getMemoryStats() {
		if (closeRequested || closed) return null;
		long closeReadLock = 0;
		try {
			//noinspection BlockingMethodInNonBlockingContext
			closeReadLock = closeLock.tryReadLock(1, TimeUnit.SECONDS);
		} catch (InterruptedException ignored) {}
		try {
			if (closeRequested || closed || closeReadLock == 0) return null;
			ensureOpen();
			return new MemoryStats(db.getAggregatedLongProperty(RocksDBLongProperty.ESTIMATE_TABLE_READERS_MEM.getName()),
					db.getAggregatedLongProperty(RocksDBLongProperty.SIZE_ALL_MEM_TABLES.getName()),
					db.getAggregatedLongProperty(RocksDBLongProperty.CUR_SIZE_ALL_MEM_TABLES.getName()),
					db.getAggregatedLongProperty(RocksDBLongProperty.ESTIMATE_NUM_KEYS.getName()),
					db.getAggregatedLongProperty(RocksDBLongProperty.BLOCK_CACHE_USAGE.getName()) / this.handles.size(),
					db.getAggregatedLongProperty(RocksDBLongProperty.BLOCK_CACHE_PINNED_USAGE.getName()) / this.handles.size(),
					db.getAggregatedLongProperty(RocksDBLongProperty.NUM_LIVE_VERSIONS.getName()) / this.handles.size()
			);
		} catch (RocksDBException e) {
			throw new DBException("Failed to read memory stats", e);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public Map<String, String> getMapProperty(@Nullable Column column, RocksDBMapProperty property) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			Map<String, String> result;
			if (column == null) {
				result = db.getMapProperty(property.getName());
			} else {
				var cfh = requireNonNull(handles.get(column));
				result = db.getMapProperty(cfh, property.getName());
			}
			return result;
		} catch (RocksDBException e) {
			if (isEmpty(e)) return null;
			throw new DBException("Failed to read property " + property.name(), e);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	private boolean isEmpty(RocksDBException ex) {
		return "NotFound".equals(ex.getMessage());
	}

	@Override
	public Stream<ColumnProperty<Map<String, String>>> getMapColumnProperties(RocksDBMapProperty property) {
		return getAllColumnFamilyHandles().keySet().stream().map(c -> new ColumnProperty<>(c.name(), property.getName(), this.getMapProperty(c, property)));
	}

	@Override
	public String getStringProperty(@Nullable Column column, RocksDBStringProperty property) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			if (column == null) {
				return db.getProperty(property.getName());
			} else {
				var cfh = requireNonNull(handles.get(column));
				return db.getProperty(cfh, property.getName());
			}
		} catch (RocksDBException e) {
			if (isEmpty(e)) return null;
			throw new DBException("Failed to read property " + property.name(), e);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public Stream<ColumnProperty<String>> getStringColumnProperties(RocksDBStringProperty property) {
		return getAllColumnFamilyHandles().keySet().stream().map(c -> {
			return new ColumnProperty<>(c.name(), property.getName(), this.getStringProperty(c, property));
		});
	}

	@Override
	public Long getLongProperty(@Nullable Column column, RocksDBLongProperty property) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			if (column == null) {
				return db.getLongProperty(property.getName());
			} else {
				var cfh = requireNonNull(handles.get(column));
				return db.getLongProperty(cfh, property.getName());
			}
		} catch (RocksDBException e) {
			if (isEmpty(e)) return null;
			throw new DBException("Failed to read property " + property.name(), e);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public Stream<ColumnProperty<Long>> getLongColumnProperties(RocksDBLongProperty property) {
		return getAllColumnFamilyHandles().keySet().stream().map(c -> {
			return new ColumnProperty<>(c.name(), property.getName(), this.getLongProperty(c, property));
		});
	}

	@Override
	public Long getAggregatedLongProperty(RocksDBLongProperty property) {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			return db.getAggregatedLongProperty(property.getName());
		} catch (RocksDBException e) {
			if (isEmpty(e)) return null;
			throw new DBException("Failed to read property " + property.name(), e);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public String getRocksDBStats() {
		if (closeRequested || closed) return null;
		long closeReadLock = 0;
		try {
			closeReadLock = closeLock.tryReadLock(1, TimeUnit.SECONDS);
		} catch (InterruptedException ignored) {}
		try {
			if (closeRequested || closed || closeReadLock == 0) return null;
			ensureOpen();
			StringBuilder aggregatedStats = new StringBuilder();
			for (var entry : this.handles.entrySet()) {
				aggregatedStats
						.append(entry.getKey().name())
						.append("\n")
						.append(db.getProperty(entry.getValue(), "rocksdb.stats"))
						.append("\n");
			}
			return aggregatedStats.toString();
		} catch (RocksDBException e) {
			throw new DBException("Failed to read stats", e);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public Stream<TableWithProperties> getTableProperties() {
		return handles.entrySet().stream().flatMap(handle -> {
			if (closeRequested || closed) {
				return null;
			}
			long closeReadLock = 0;
			try {
				closeReadLock = closeLock.tryReadLock(1, TimeUnit.SECONDS);
			} catch (InterruptedException ignored) {
			}
			try {
				if (closeRequested || closed || closeReadLock == 0) {
					return null;
				}
				ensureOpen();
				return db
						.getPropertiesOfAllTables(handle.getValue())
						.entrySet()
						.stream()
						.map(entry -> new TableWithProperties(handle.getKey().name(), entry.getKey(), entry.getValue()));
			} catch (RocksDBException e) {
				throw new CompletionException(new DBException("Failed to read stats", e));
			} finally {
				closeLock.unlockRead(closeReadLock);
			}
		});
	}

	@Override
	public void verifyChecksum() {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			db.verifyChecksum();
		} catch (RocksDBException e) {
			throw new DBException("Failed to verify checksum of database \"" + getDatabaseName() + "\"", e);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void compact() {
		this.forceCompaction(getLastVolumeId());
	}

	@Override
	public void flush() {
		try (var fo = new FlushOptions().setWaitForFlush(true)) {
			this.flush(fo);
		} catch (RocksDBException ex) {
			if (!"ShutdownInProgress".equals(ex.getMessage())) {
				throw new DBException(ex);
			}
			logger.warn("Shutdown in progress. Flush cancelled", ex);
		}
	}

	@Override
	public MeterRegistry getMeterRegistry() {
		return meterRegistry;
	}

	@Override
	public LLSnapshot takeSnapshot() {
		if (!ALLOW_SNAPSHOTS) throw new UnsupportedOperationException("Snapshots are disabled!");
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			return snapshotTime.record(() -> {
				var snapshot = db.getSnapshot();
				long currentSnapshotSequenceNumber = nextSnapshotNumbers.getAndIncrement();
				this.snapshotsHandles.put(currentSnapshotSequenceNumber, snapshot);
				return new LLSnapshot(currentSnapshotSequenceNumber);
			});
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void releaseSnapshot(LLSnapshot snapshot) {
		var closeReadLock = closeLock.readLock();
		try (var dbSnapshot = this.snapshotsHandles.remove(snapshot.getSequenceNumber())) {
			if (dbSnapshot == null) {
				throw new DBException("Snapshot " + snapshot.getSequenceNumber() + " not found!");
			}
			if (!db.isOwningHandle()) {
				return;
			}
			db.releaseSnapshot(dbSnapshot);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	@Override
	public void close() {
		closeRequested = true;
		if (statistics != null) {
			try {
				statistics.close();
				statistics = null;
			} catch (Exception ex) {
				logger.error("Failed to close db statistics", ex);
			}
		}
		try {
			flushAndCloseDb(db,
					standardCache,
					new ArrayList<>(handles.values())
			);
			handles.values().forEach(columnFamilyHandleRocksObj -> {
				if (LLUtils.isAccessible(columnFamilyHandleRocksObj)) {
					columnFamilyHandleRocksObj.close();
				}
			});
			handles.clear();
			deleteUnusedOldLogFiles();
		} catch (Exception e) {
			throw new DBException("Failed to close", e);
		} finally {
			if (dbReadPool != null) {
				try {
					dbReadPool.close();
					dbReadPool = null;
				} catch (Exception ex) {
					logger.error("Failed to close db pool", ex);
				}
			}
			if (dbWritePool != null) {
				try {
					dbWritePool.close();
					dbWritePool = null;
				} catch (Exception ex) {
					logger.error("Failed to close db pool", ex);
				}
			}
		}
	}

	private void pauseWrites() {
		try {
			db.pauseBackgroundWork();
			db.disableFileDeletions();
		} catch (RocksDBException e) {
			throw new DBException(e);
		}
	}

	private void resumeWrites() {
		try {
			db.continueBackgroundWork();
			db.enableFileDeletions();
		} catch (RocksDBException e) {
			throw new DBException(e);
		}
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
			try (var f = Files.walk(basePath, 1)) {
				f.filter(p -> !p.equals(basePath)).filter(p -> {
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
				}).filter(p -> {
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
				}).forEach(path -> {
					try {
						Files.deleteIfExists(path);
						System.out.println("Deleted log file \"" + path + "\"");
					} catch (IOException e) {
						logger.error(MARKER_ROCKSDB, "Failed to delete log file \"" + path + "\"", e);
					}
				});
			}
		} catch (IOException ex) {
			logger.error(MARKER_ROCKSDB, "Failed to delete unused log files", ex);
		}
	}

}
