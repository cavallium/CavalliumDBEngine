package it.cavallium.dbengine.database;

import it.cavallium.dbengine.client.MemoryStats;
import it.cavallium.dbengine.rpc.current.data.Column;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface DatabaseProperties {

	Mono<MemoryStats> getMemoryStats();

	Mono<String> getRocksDBStats();

	Mono<Map<String, String>> getMapProperty(@Nullable Column column, RocksDBMapProperty property);

	Flux<ColumnProperty<Map<String, String>>> getMapColumnProperties(RocksDBMapProperty property);

	Mono<String> getStringProperty(@Nullable Column column, RocksDBStringProperty property);

	Flux<ColumnProperty<String>> getStringColumnProperties(RocksDBStringProperty property);

	Mono<Long> getLongProperty(@Nullable Column column, RocksDBLongProperty property);

	Flux<ColumnProperty<Long>> getLongColumnProperties(RocksDBLongProperty property);

	Mono<Long> getAggregatedLongProperty(RocksDBLongProperty property);

	Flux<TableWithProperties> getTableProperties();
}
