package it.cavallium.dbengine.database;

import it.cavallium.dbengine.client.MemoryStats;
import it.cavallium.dbengine.rpc.current.data.Column;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

public interface DatabaseProperties {

	MemoryStats getMemoryStats();

	String getRocksDBStats();

	Map<String, String> getMapProperty(@Nullable Column column, RocksDBMapProperty property);

	Stream<ColumnProperty<Map<String, String>>> getMapColumnProperties(RocksDBMapProperty property);

	String getStringProperty(@Nullable Column column, RocksDBStringProperty property);

	Stream<ColumnProperty<String>> getStringColumnProperties(RocksDBStringProperty property);

	Long getLongProperty(@Nullable Column column, RocksDBLongProperty property);

	Stream<ColumnProperty<Long>> getLongColumnProperties(RocksDBLongProperty property);

	Long getAggregatedLongProperty(RocksDBLongProperty property);

	Stream<TableWithProperties> getTableProperties();
}
