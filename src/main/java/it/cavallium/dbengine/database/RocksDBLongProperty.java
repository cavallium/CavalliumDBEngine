package it.cavallium.dbengine.database;

public enum RocksDBLongProperty implements RocksDBProperty {
	NUM_FILES_AT_LEVEL_0("num-files-at-level0"),
	NUM_FILES_AT_LEVEL_1("num-files-at-level1"),
	NUM_FILES_AT_LEVEL_2("num-files-at-level2"),
	NUM_FILES_AT_LEVEL_3("num-files-at-level3"),
	NUM_FILES_AT_LEVEL_4("num-files-at-level4"),
	NUM_FILES_AT_LEVEL_5("num-files-at-level5"),
	NUM_FILES_AT_LEVEL_6("num-files-at-level6"),
	NUM_FILES_AT_LEVEL_7("num-files-at-level7"),
	NUM_FILES_AT_LEVEL_8("num-files-at-level8"),
	NUM_FILES_AT_LEVEL_9("num-files-at-level9"),
	COMPRESSION_RATIO_AT_LEVEL_0("compression-ratio-at-level0"),
	COMPRESSION_RATIO_AT_LEVEL_1("compression-ratio-at-level1"),
	COMPRESSION_RATIO_AT_LEVEL_2("compression-ratio-at-level2"),
	COMPRESSION_RATIO_AT_LEVEL_3("compression-ratio-at-level3"),
	COMPRESSION_RATIO_AT_LEVEL_4("compression-ratio-at-level4"),
	COMPRESSION_RATIO_AT_LEVEL_5("compression-ratio-at-level5"),
	COMPRESSION_RATIO_AT_LEVEL_6("compression-ratio-at-level6"),
	COMPRESSION_RATIO_AT_LEVEL_7("compression-ratio-at-level7"),
	COMPRESSION_RATIO_AT_LEVEL_8("compression-ratio-at-level8"),
	COMPRESSION_RATIO_AT_LEVEL_9("compression-ratio-at-level9"),
	NUM_IMMUTABLE_MEM_TABLE("num-immutable-mem-table"),
	NUM_IMMUTABLE_MEM_TABLE_FLUSHED("num-immutable-mem-table-flushed"),
	MEM_TABLE_FLUSH_PENDING("mem-table-flush-pending"),
	NUM_RUNNING_FLUSHES("num-running-flushes"),
	COMPACTION_PENDING("compaction-pending"),
	NUM_RUNNING_COMPACTIONS("num-running-compactions"),
	BACKGROUND_ERRORS("background-errors"),
	CUR_SIZE_ACTIVE_MEM_TABLE("cur-size-active-mem-table"),
	CUR_SIZE_ALL_MEM_TABLES("cur-size-all-mem-tables"),
	SIZE_ALL_MEM_TABLES("size-all-mem-tables"),
	NUM_ENTRIES_ACTIVE_MEM_TABLE("num-entries-active-mem-table"),
	NUM_ENTRIES_IMMUTABLE_MEM_TABLES("num-entries-imm-mem-tables"),
	NUM_DELETES_ACTIVE_MEM_TABLE("num-deletes-active-mem-table"),
	NUM_DELETES_IMMUTABLE_MEM_TABLES("num-deletes-imm-mem-tables"),
	ESTIMATE_NUM_KEYS("estimate-num-keys"),
	ESTIMATE_TABLE_READERS_MEM("estimate-table-readers-mem"),
	IS_FILE_DELETIONS_ENABLED("is-file-deletions-enabled"),
	NUM_SNAPSHOTS("num-snapshots"),
	OLDEST_SNAPSHOT_TIME("oldest-snapshot-time"),
	OLDEST_SNAPSHOT_SEQUENCE("oldest-snapshot-sequence"),
	NUM_LIVE_VERSIONS("num-live-versions"),
	CURRENT_SUPER_VERSION_NUMBER("current-super-version-number"),
	ESTIMATE_LIVE_DATA_SIZE("estimate-live-data-size"),
	MIN_LOG_NUMBER_TO_KEEP("min-log-number-to-keep"),
	MIN_OBSOLETE_SST_NUMBER_TO_KEEP("min-obsolete-sst-number-to-keep"),
	TOTAL_SST_FILES_SIZE("total-sst-files-size"),
	LIVE_SST_FILES_SIZE("live-sst-files-size"),
	LIVE_SST_FILES_SIZE_AT_TEMPERATURE("live-sst-files-size-at-temperature"),
	BASE_LEVEL("base-level"),
	ESTIMATE_PENDING_COMPACTION_BYTES("estimate-pending-compaction-bytes"),
	ACTUAL_DELAYED_WRITE_RATE("actual-delayed-write-rate"),
	IS_WRITE_STOPPED("is-write-stopped"),
	ESTIMATE_OLDEST_KEY_TIME("estimate-oldest-key-time"),
	BLOCK_CACHE_CAPACITY("block-cache-capacity", false),
	BLOCK_CACHE_USAGE("block-cache-usage", false),
	BLOCK_CACHE_PINNED_USAGE("block-cache-pinned-usage", false),
	NUM_BLOB_FILES("num-blob-files"),
	TOTAL_BLOB_FILE_SIZE("total-blob-file-size"),
	LIVE_BLOB_FILE_SIZE("live-blob-file-size"),
	LIVE_BLOB_FILE_GARBAGE_SIZE("live-blob-file-garbage-size"),
	FILE_READ_DB_OPEN_MICROS("file.read.db.open.micros")
	;

	private final String name;
	private final boolean dividedByColumnFamily;

	RocksDBLongProperty(String name) {
		this(name, true);
	}

	RocksDBLongProperty(String name, boolean dividedByColumnFamily) {
		this.name = name;
		this.dividedByColumnFamily = dividedByColumnFamily;
	}

	@Override
	public String toString() {
		return "rocksdb." + name;
	}

	@Override
	public String getName() {
		return "rocksdb." + name;
	}

	@Override
	public boolean isNumeric() {
		return true;
	}

	@Override
	public boolean isMap() {
		return false;
	}

	@Override
	public boolean isString() {
		return false;
	}

	public boolean isDividedByColumnFamily() {
		return dividedByColumnFamily;
	}
}
