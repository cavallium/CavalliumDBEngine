package it.cavallium.dbengine.database.disk;

import static com.google.common.collect.Lists.partition;
import static io.netty5.buffer.api.StandardAllocationTypes.OFF_HEAP;
import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static org.rocksdb.ColumnFamilyOptionsInterface.DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.util.internal.PlatformDependent;
import it.cavallium.data.generator.nativedata.NullableString;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompactionOptions;
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
import org.rocksdb.LRUCache;
import org.rocksdb.LevelMetaData;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.PersistentCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.SstFileMetaData;
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

	private final BufferAllocator allocator;
	private final MeterRegistry meterRegistry;
	private final Scheduler dbWScheduler;
	private final Scheduler dbRScheduler;

	private final Timer snapshotTime;

	// Configurations

	private final Path dbPath;
	private final String name;
	private final DatabaseOptions databaseOptions;

	private final boolean enableColumnsBug;
	private RocksDB db;
	private Cache standardCache;
	private Cache compressedCache;
	private final Map<Column, ColumnFamilyHandle> handles;

	private final HashMap<String, PersistentCache> persistentCaches;
	private final ConcurrentHashMap<Long, Snapshot> snapshotsHandles = new ConcurrentHashMap<>();
	private final AtomicLong nextSnapshotNumbers = new AtomicLong(1);
	private final StampedLock closeLock = new StampedLock();
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
		boolean nettyDirect = databaseOptions.allowNettyDirect() && allocator.getAllocationType() == OFF_HEAP;
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

		this.enableColumnsBug = "true".equals(databaseOptions.extraFlags().getOrDefault("enableColumnBug", "false"));

		if (!enableColumnsBug) {
			if (columns.stream().noneMatch(column -> column.name().equals("default"))) {
				columns = Stream.concat(Stream.of(Column.of("default")), columns.stream()).toList();
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

			this.persistentCaches = new HashMap<>();

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

				if (isDisableAutoCompactions()) {
					columnFamilyOptions.setDisableAutoCompactions(true);
				}

				// This option is not supported with multiple db paths
				// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
				// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
				boolean dynamicLevelBytes = databaseOptions.volumes().size() <= 1;
				if (dynamicLevelBytes) {
					columnFamilyOptions.setLevelCompactionDynamicLevelBytes(true);
				} else {
					// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
					// https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
					columnFamilyOptions.setMaxBytesForLevelBase(256 * SizeUnit.MB);
					// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
					columnFamilyOptions.setMaxBytesForLevelMultiplier(10);
				}
				if (isDisableAutoCompactions()) {
					columnFamilyOptions.setLevel0FileNumCompactionTrigger(-1);
				} else {
					// ArangoDB uses a value of 2: https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
					// Higher values speed up writes, but slow down reads
					columnFamilyOptions.setLevel0FileNumCompactionTrigger(4);
				}
				if (isDisableSlowdown()) {
					columnFamilyOptions.setLevel0SlowdownWritesTrigger(-1);
					columnFamilyOptions.setLevel0StopWritesTrigger(Integer.MAX_VALUE);
					columnFamilyOptions.setHardPendingCompactionBytesLimit(Long.MAX_VALUE);
					columnFamilyOptions.setSoftPendingCompactionBytesLimit(Long.MAX_VALUE);
				} else {
					// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
					columnFamilyOptions.setLevel0SlowdownWritesTrigger(20);
					// https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
					columnFamilyOptions.setLevel0StopWritesTrigger(36);
				}

				if (!columnOptions.levels().isEmpty()) {
					columnFamilyOptions.setNumLevels(columnOptions.levels().size());
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
					columnFamilyOptions.setBottommostCompressionType(CompressionType.LZ4HC_COMPRESSION);
					columnFamilyOptions.setBottommostCompressionOptions(new CompressionOptions()
							.setEnabled(true)
							.setMaxDictBytes(32768));
					columnFamilyOptions.setCompressionPerLevel(compressionTypes);
				}

				final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
				if (!databaseOptions.lowMemory()) {
					// tableOptions.setOptimizeFiltersForMemory(true);
					columnFamilyOptions.setWriteBufferSize(256 * SizeUnit.MB);
				}
				if (columnOptions.writeBufferSize().isPresent()) {
					columnFamilyOptions.setWriteBufferSize(columnOptions.writeBufferSize().get());
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
					// cacheIndexAndFilterBlocks = true;
					// https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
					columnFamilyOptions.setMinWriteBufferNumberToMerge(3);
					// https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
					columnFamilyOptions.setMaxWriteBufferNumber(4);
				}
				tableOptions
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
						//todo: replace with kxxhash3
						.setChecksumType(ChecksumType.kCRC32c)
						// Spinning disks: 64KiB to 256KiB (also 512KiB). SSDs: 16KiB
						// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
						// https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
						.setBlockSize(columnOptions.blockSize().orElse((databaseOptions.spinning() ? 128 : 16) * 1024))
						.setBlockCacheCompressed(optionsWithCache.compressedCache())
						.setBlockCache(optionsWithCache.standardCache())
						.setPersistentCache(resolvePersistentCache(persistentCaches, rocksdbOptions, databaseOptions.persistentCaches(), columnOptions.persistentCacheId()));

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
				columnFamilyOptions.setTargetFileSizeBase(64 * SizeUnit.MB);
				// // For each level up, the threshold is multiplied by the factor target_file_size_multiplier
				// // (but the default value is 1, which means that the maximum sstable of each level is the same).
				columnFamilyOptions.setTargetFileSizeMultiplier(2);

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

	public static boolean isDisableAutoCompactions() {
		return Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.compactions.auto.disable", "false"));
	}

	public static boolean isDisableSlowdown() {
		return isDisableAutoCompactions()
				|| Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.disableslowdown", "false"));
	}

	protected void ensureOpen() {
		if (closed) {
			throw new IllegalStateException("Database closed");
		}
		RocksDBUtils.ensureOpen(db, null);
	}

	protected void ensureOwned(org.rocksdb.RocksObject rocksObject) {
		RocksDBUtils.ensureOwned(rocksObject);
	}

	private synchronized PersistentCache resolvePersistentCache(HashMap<String, PersistentCache> caches,
			DBOptions rocksdbOptions,
			List<it.cavallium.dbengine.rpc.current.data.PersistentCache> persistentCaches,
			NullableString persistentCacheId) throws RocksDBException {
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
					new RocksLog4jLogger(rocksdbOptions, logger),
					foundCache.optimizeForNvm()
			);
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

	public List<String> getColumnFiles(Column column, boolean excludeLastLevel) {
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

	public void forceCompaction(int volumeId) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			for (var cfh : this.handles.values()) {
				ensureOwned(cfh);
				RocksDBUtils.forceCompaction(db, name, cfh, volumeId, logger);
			}
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
	}

	public void flush(FlushOptions flushOptions) throws RocksDBException {
		var closeReadLock = closeLock.readLock();
		try {
			ensureOpen();
			ensureOwned(flushOptions);
			db.flush(flushOptions);
		} finally {
			closeLock.unlockRead(closeReadLock);
		}
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
					var closeReadLock = closeLock.readLock();
					try {
						if (closed) {
							return 0d;
						}
						return database.getAggregatedLongProperty(propertyName)
								/ (divideByAllColumns ? getAllColumnFamilyHandles().size() : 1d);
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

	@Override
	public String getDatabaseName() {
		return name;
	}

	public StampedLock getCloseLock() {
		return closeLock;
	}

	private void flushAndCloseDb(RocksDB db, Cache standardCache, Cache compressedCache, List<ColumnFamilyHandle> handles)
			throws RocksDBException {
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
			if (compressedCache != null) {
				compressedCache.close();
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
		} finally {
			closeLock.unlockWrite(closeWriteLock);
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
		options.setMaxSubcompactions(Integer.parseInt(System.getProperty("it.cavallium.dbengine.compactions.max.sub", "2")));
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
		List<DbPath> paths = convertPaths(databasesDirPath, path.getFileName(), databaseOptions.volumes())
				.stream()
				.map(p -> new DbPath(p.path, p.targetSize))
				.toList();
		options.setDbPaths(paths);
		options.setMaxOpenFiles(databaseOptions.maxOpenFiles().orElse(-1));
		if (databaseOptions.spinning()) {
			// https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
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
			options.setMaxBackgroundJobs(Integer.parseInt(System.getProperty("it.cavallium.dbengine.jobs.background.num", "2")));
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
			blockCache = new LRUCache(writeBufferManagerSize + databaseOptions.blockCache().orElse( 8L * SizeUnit.MB));
			if (databaseOptions.compressedBlockCache().isPresent()) {
				compressedCache = new LRUCache(databaseOptions.compressedBlockCache().get());
			} else {
				compressedCache = null;
			}

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
					//.setDbWriteBufferSize(64 * SizeUnit.MB)
					.setBytesPerSync(0)
					.setWalBytesPerSync(0)

					.setWalTtlSeconds(0) // Auto
					.setWalSizeLimitMB(0) // Auto
					.setMaxTotalWalSize(0) // Auto
			;
			// DO NOT USE ClockCache! IT'S BROKEN!
			blockCache = new LRUCache(writeBufferManagerSize + databaseOptions.blockCache().orElse( 512 * SizeUnit.MB));
			if (databaseOptions.compressedBlockCache().isPresent()) {
				compressedCache = new LRUCache(databaseOptions.compressedBlockCache().get());
			} else {
				compressedCache = null;
			}

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

		if (databaseOptions.writeBufferManager().isPresent()) {
			options.setWriteBufferManager(new WriteBufferManager(writeBufferManagerSize, blockCache));
		}

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

	private Snapshot getSnapshotLambda(LLSnapshot snapshot) {
		var closeReadSnapLock = closeLock.readLock();
		try {
			ensureOpen();
			var snapshotHandle = snapshotsHandles.get(snapshot.getSequenceNumber());
			ensureOwned(snapshotHandle);
			return snapshotHandle;
		} finally {
			closeLock.unlockRead(closeReadSnapLock);
		}
	}

	@Override
	public Mono<LLLocalSingleton> getSingleton(byte[] singletonListColumnName,
			byte[] name,
			byte @Nullable[] defaultValue) {
		return Mono
				.fromCallable(() -> {
					var closeReadLock = closeLock.readLock();
					try {
						ensureOpen();
						var cfh = getCfh(singletonListColumnName);
						ensureOwned(cfh);
						return new LLLocalSingleton(
								getRocksDBColumn(db, cfh),
								this::getSnapshotLambda,
								LLLocalKeyValueDatabase.this.name,
								name,
								ColumnUtils.toString(singletonListColumnName),
								dbWScheduler, dbRScheduler, defaultValue
						);
					} finally {
						closeLock.unlockRead(closeReadLock);
					}
				})
				.onErrorMap(cause -> new IOException("Failed to read " + Arrays.toString(name), cause))
				.subscribeOn(dbRScheduler);
	}

	@Override
	public Mono<LLLocalDictionary> getDictionary(byte[] columnName, UpdateMode updateMode) {
		return Mono
				.fromCallable(() -> {
					var closeReadLock = closeLock.readLock();
					try {
						ensureOpen();
						var cfh = getCfh(columnName);
						ensureOwned(cfh);
						return new LLLocalDictionary(
								allocator,
								getRocksDBColumn(db, cfh),
								name,
								ColumnUtils.toString(columnName),
								dbWScheduler,
								dbRScheduler,
								this::getSnapshotLambda,
								updateMode,
								databaseOptions
						);
					} finally {
						closeLock.unlockRead(closeReadLock);
					}
				})
				.subscribeOn(dbRScheduler);
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
		var nettyDirect = databaseOptions.allowNettyDirect();
		var closeLock = getCloseLock();
		if (db instanceof OptimisticTransactionDB optimisticTransactionDB) {
			return new OptimisticRocksDBColumn(optimisticTransactionDB,
					nettyDirect,
					allocator,
					name,
					cfh,
					meterRegistry,
					closeLock
			);
		} else if (db instanceof TransactionDB transactionDB) {
			return new PessimisticRocksDBColumn(transactionDB,
					nettyDirect,
					allocator,
					name,
					cfh,
					meterRegistry,
					closeLock
			);
		} else {
			return new StandardRocksDBColumn(db, nettyDirect, allocator, name, cfh, meterRegistry, closeLock);
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
		return Mono.fromCallable(() -> {
			var closeReadLock = closeLock.readLock();
			try {
				ensureOpen();
				return db.getAggregatedLongProperty(propertyName);
			} finally {
				closeLock.unlockRead(closeReadLock);
			}
		}).onErrorMap(cause -> new IOException("Failed to read " + propertyName, cause)).subscribeOn(dbRScheduler);
	}

	public Flux<Path> getSSTS() {
		var paths = convertPaths(dbPath.toAbsolutePath().getParent(), dbPath.getFileName(), databaseOptions.volumes());
		return Mono
				.fromCallable(() -> {
					var closeReadLock = closeLock.readLock();
					try {
						ensureOpen();
						return db.getLiveFiles();
					} finally {
						closeLock.unlockRead(closeReadLock);
					}
				})
				.flatMapIterable(liveFiles -> liveFiles.files)
				.filter(file -> file.endsWith(".sst"))
				.map(file -> file.substring(1))
				.flatMapSequential(file -> Mono.fromCallable(() -> {
					{
						var path = dbPath.resolve(file);
						if (Files.exists(path)) {
							return path;
						}
					}
					for (var volumePath : paths) {
						var path = volumePath.path().resolve(file);
						if (Files.exists(path)) {
							return path;
						}
					}
					return null;
				}).subscribeOn(Schedulers.boundedElastic()));
	}

	public Mono<Void> ingestSSTS(Flux<Path> sstsFlux) {
		return sstsFlux
				.map(path -> path.toAbsolutePath().toString())
				.flatMap(sst -> Mono.fromCallable(() -> {
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
					return null;
				}).subscribeOn(Schedulers.boundedElastic()))
				.then();
	}

	@Override
	public Mono<MemoryStats> getMemoryStats() {
		return Mono
				.fromCallable(() -> {
					if (closed) return null;
					var closeReadLock = closeLock.readLock();
					try {
						if (!closed) {
							ensureOpen();
							return new MemoryStats(db.getAggregatedLongProperty("rocksdb.estimate-table-readers-mem"),
									db.getAggregatedLongProperty("rocksdb.size-all-mem-tables"),
									db.getAggregatedLongProperty("rocksdb.cur-size-all-mem-tables"),
									db.getAggregatedLongProperty("rocksdb.estimate-num-keys"),
									db.getAggregatedLongProperty("rocksdb.block-cache-usage") / this.handles.size(),
									db.getAggregatedLongProperty("rocksdb.block-cache-pinned-usage") / this.handles.size()
							);
						} else {
							return null;
						}
					} finally {
						closeLock.unlockRead(closeReadLock);
					}
				})
				.onErrorMap(cause -> new IOException("Failed to read memory stats", cause))
				.subscribeOn(dbRScheduler);
	}

	@Override
	public Mono<String> getRocksDBStats() {
		return Mono
				.fromCallable(() -> {
					if (closed) return null;
					var closeReadLock = closeLock.readLock();
					try {
						if (!closed) {
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
						} else {
							return null;
						}
					} finally {
						closeLock.unlockRead(closeReadLock);
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
							if (closed) return null;
							var closeReadLock = closeLock.readLock();
							try {
								if (closed) return null;
								ensureOpen();
								return db.getPropertiesOfAllTables(handle.getValue());
							} finally {
								closeLock.unlockRead(closeReadLock);
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
					var closeReadLock = closeLock.readLock();
					try {
						ensureOpen();
						db.verifyChecksum();
					} finally {
						closeLock.unlockRead(closeReadLock);
					}
					return null;
				})
				.onErrorMap(cause -> new IOException("Failed to verify checksum of database \""
						+ getDatabaseName() + "\"", cause))
				.subscribeOn(dbRScheduler);
	}

	@Override
	public Mono<Void> compact() {
		return Mono.<Void>fromCallable(() -> {
			this.forceCompaction(getLastVolumeId());
			return null;
		}).subscribeOn(dbWScheduler);
	}

	@Override
	public Mono<Void> flush() {
		return Mono.<Void>fromCallable(() -> {
			try (var fo = new FlushOptions().setWaitForFlush(true)) {
				this.flush(fo);
				return null;
			}
		}).subscribeOn(dbWScheduler);
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
		return Mono.fromCallable(() -> {
			var closeReadLock = closeLock.readLock();
			try {
				ensureOpen();
				return snapshotTime.recordCallable(() -> {
					var snapshot = db.getSnapshot();
					long currentSnapshotSequenceNumber = nextSnapshotNumbers.getAndIncrement();
					this.snapshotsHandles.put(currentSnapshotSequenceNumber, snapshot);
					return new LLSnapshot(currentSnapshotSequenceNumber);
				});
			} finally {
				closeLock.unlockRead(closeReadLock);
			}
		}).subscribeOn(dbRScheduler);
	}

	@Override
	public Mono<Void> releaseSnapshot(LLSnapshot snapshot) {
		return Mono
				.<Void>fromCallable(() -> {
					var closeReadLock = closeLock.readLock();
					try {
						Snapshot dbSnapshot = this.snapshotsHandles.remove(snapshot.getSequenceNumber());
						if (dbSnapshot == null) {
							throw new IOException("Snapshot " + snapshot.getSequenceNumber() + " not found!");
						}
						if (!db.isOwningHandle()) {
							return null;
						}
						db.releaseSnapshot(dbSnapshot);
						return null;
					} finally {
						closeLock.unlockRead(closeReadLock);
					}
				})
				.subscribeOn(dbRScheduler);
	}

	@Override
	public Mono<Void> close() {
		return Mono
				.<Void>fromCallable(() -> {
					try {
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
