package it.cavallium.dbengine.database;

public enum RocksDBMapProperty implements RocksDBProperty {
	CFSTATS("cfstats"),
	DBSTATS("dbstats"),
	BLOCK_CACHE_ENTRY_STATS("block-cache-entry-stats"),
	AGGREGATED_TABLE_PROPERTIES("aggregated-table-properties"),
	;

	private final String name;

	RocksDBMapProperty(String name) {
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
		return true;
	}

	@Override
	public boolean isString() {
		return false;
	}
}
