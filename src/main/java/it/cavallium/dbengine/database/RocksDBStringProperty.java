package it.cavallium.dbengine.database;

public enum RocksDBStringProperty implements RocksDBProperty {
	STATS("stats"),
	SSTABLES("sstables"),
	CFSTATS_NO_FILE_HISTOGRAM("cfstats-no-file-histogram"),
	CF_FILE_HISTOGRAM("cf-file-histogram"),
	LEVELSTATS("levelstats"),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_0("aggregated-table-properties-at-level0"),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_1("aggregated-table-properties-at-level1"),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_2("aggregated-table-properties-at-level2"),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_3("aggregated-table-properties-at-level3"),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_4("aggregated-table-properties-at-level4"),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_5("aggregated-table-properties-at-level5"),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_6("aggregated-table-properties-at-level6"),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_7("aggregated-table-properties-at-level7"),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_8("aggregated-table-properties-at-level8"),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_9("aggregated-table-properties-at-level9"),
	OPTIONS_STATISTICS("options-statistics"),
	BLOB_STATS("blob-stats")
	;

	private final String name;

	RocksDBStringProperty(String name) {
		this.name = name;
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
		return false;
	}

	@Override
	public boolean isMap() {
		return false;
	}

	@Override
	public boolean isString() {
		return true;
	}
}
