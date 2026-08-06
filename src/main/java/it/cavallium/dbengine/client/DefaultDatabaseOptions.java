package it.cavallium.dbengine.client;

import it.cavallium.dbengine.rpc.current.data.ColumnOptions;
import it.cavallium.dbengine.rpc.current.data.DatabaseLevel;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.DatabaseVolume;
import it.cavallium.dbengine.rpc.current.data.NamedColumnOptions;
import it.cavallium.dbengine.rpc.current.data.PersistentCache;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.rocksdb.RocksDB;

public class DefaultDatabaseOptions {

	public static ColumnOptions DEFAULT_DEFAULT_COLUMN_OPTIONS = ColumnOptions.of(
			new DatabaseLevel[0],
			false,
			0L,
			false,
			false,
			false,
			false,
			null,
			false,
			0,
			null,
			false,
			0L,
			false,
			false,
			0L,
			false,
			0L,
			null
	);

	public static NamedColumnOptions DEFAULT_NAMED_COLUMN_OPTIONS = NamedColumnOptions.of(
			new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8),
			DEFAULT_DEFAULT_COLUMN_OPTIONS
	);

	public static DatabaseOptions DEFAULT_DATABASE_OPTIONS = DatabaseOptions.of(new DatabaseVolume[0],
			Map.of(),
			false,
			false,
			false,
			false,
			true,
			false,
			0,
			false,
			0L,
			new PersistentCache[0],
			false,
			0L,
			false,
			DEFAULT_DEFAULT_COLUMN_OPTIONS,
			new NamedColumnOptions[0],
			null,
			null,
			false,
			null
	);

	public static DatabaseOptions.Builder builder() {
		return DEFAULT_DATABASE_OPTIONS.builder();
	}

	public static ColumnOptions.Builder defaultColumnOptionsBuilder() {
		return DEFAULT_DEFAULT_COLUMN_OPTIONS.builder();
	}

	public static NamedColumnOptions.Builder namedColumnOptionsBuilder() {
		return DEFAULT_NAMED_COLUMN_OPTIONS.builder();
	}
}
