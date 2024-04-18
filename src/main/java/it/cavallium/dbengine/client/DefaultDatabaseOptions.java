package it.cavallium.dbengine.client;

import it.cavallium.datagen.nativedata.NullableString;
import it.cavallium.datagen.nativedata.Nullableboolean;
import it.cavallium.datagen.nativedata.Nullableint;
import it.cavallium.datagen.nativedata.Nullablelong;
import it.cavallium.dbengine.rpc.current.data.ColumnOptions;
import it.cavallium.dbengine.rpc.current.data.ColumnOptionsBuilder;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptionsBuilder;
import it.cavallium.dbengine.rpc.current.data.NamedColumnOptions;
import it.cavallium.dbengine.rpc.current.data.NamedColumnOptionsBuilder;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableCompression;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableFilter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.rocksdb.RocksDB;

public class DefaultDatabaseOptions {

	public static ColumnOptions DEFAULT_DEFAULT_COLUMN_OPTIONS = new ColumnOptions(
			Collections.emptyList(),
			Nullablelong.empty(),
			Nullableboolean.empty(),
			Nullableboolean.empty(),
			NullableFilter.empty(),
			Nullableint.empty(),
			NullableString.empty(),
			Nullablelong.empty(),
			false,
			Nullablelong.empty(),
			Nullablelong.empty(),
			NullableCompression.empty()
	);

	public static NamedColumnOptions DEFAULT_NAMED_COLUMN_OPTIONS = new NamedColumnOptions(
			new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8),
			DEFAULT_DEFAULT_COLUMN_OPTIONS
	);

	public static DatabaseOptions DEFAULT_DATABASE_OPTIONS = new DatabaseOptions(List.of(),
			Map.of(),
			false,
			false,
			false,
			false,
			true,
			Nullableint.empty(),
			Nullablelong.empty(),
			Collections.emptyList(),
			Nullablelong.empty(),
			false,
			DEFAULT_DEFAULT_COLUMN_OPTIONS,
			List.of(),
			NullableString.empty(),
			NullableString.empty(),
			false,
			NullableString.empty()
	);

	public static DatabaseOptionsBuilder builder() {
		return DatabaseOptionsBuilder.builder(DEFAULT_DATABASE_OPTIONS);
	}

	public static ColumnOptionsBuilder defaultColumnOptionsBuilder() {
		return ColumnOptionsBuilder.builder(DEFAULT_DEFAULT_COLUMN_OPTIONS);
	}

	public static NamedColumnOptionsBuilder namedColumnOptionsBuilder() {
		return NamedColumnOptionsBuilder.builder(DEFAULT_NAMED_COLUMN_OPTIONS);
	}
}
