package it.cavallium.dbengine.client;

import it.cavallium.data.generator.nativedata.Nullableboolean;
import it.cavallium.data.generator.nativedata.Nullableint;
import it.cavallium.data.generator.nativedata.Nullablelong;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptionsBuilder;
import it.cavallium.dbengine.rpc.current.data.DefaultColumnOptions;
import it.cavallium.dbengine.rpc.current.data.DefaultColumnOptionsBuilder;
import it.cavallium.dbengine.rpc.current.data.NamedColumnOptions;
import it.cavallium.dbengine.rpc.current.data.NamedColumnOptionsBuilder;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableFilter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.rocksdb.RocksDB;

public class DefaultDatabaseOptions {

	public static DefaultColumnOptions DEFAULT_DEFAULT_COLUMN_OPTIONS = new DefaultColumnOptions(
			Collections.emptyList(),
			Nullablelong.empty(),
			Nullableboolean.empty(),
			NullableFilter.empty(),
			Nullableint.empty()
	);

	public static NamedColumnOptions DEFAULT_NAMED_COLUMN_OPTIONS = new NamedColumnOptions(
			new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8),
			Collections.emptyList(),
			Nullablelong.empty(),
			Nullableboolean.empty(),
			NullableFilter.empty(),
			Nullableint.empty()
	);

	public static DatabaseOptions DEFAULT_DATABASE_OPTIONS = new DatabaseOptions(List.of(),
			Map.of(),
			false,
			false,
			false,
			false,
			true,
			true,
			Nullableint.empty(),
			Nullablelong.empty(),
			Nullablelong.empty(),
			Nullablelong.empty(),
			false,
			DEFAULT_DEFAULT_COLUMN_OPTIONS,
			List.of()
	);

	public static DatabaseOptionsBuilder builder() {
		return DatabaseOptionsBuilder.builder(DEFAULT_DATABASE_OPTIONS);
	}

	public static DefaultColumnOptionsBuilder defaultColumnOptionsBuilder() {
		return DefaultColumnOptionsBuilder.builder(DEFAULT_DEFAULT_COLUMN_OPTIONS);
	}

	public static NamedColumnOptionsBuilder namedColumnOptionsBuilder() {
		return NamedColumnOptionsBuilder.builder(DEFAULT_NAMED_COLUMN_OPTIONS);
	}
}
