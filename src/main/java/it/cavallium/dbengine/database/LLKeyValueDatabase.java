package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty5.buffer.BufferAllocator;
import it.cavallium.dbengine.client.IBackuppable;
import it.cavallium.dbengine.client.MemoryStats;
import it.cavallium.dbengine.database.collections.DatabaseInt;
import it.cavallium.dbengine.database.collections.DatabaseLong;
import it.cavallium.dbengine.rpc.current.data.Column;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface LLKeyValueDatabase extends LLSnapshottable, LLKeyValueDatabaseStructure, DatabaseProperties,
		IBackuppable, DatabaseOperations {

	Mono<? extends LLSingleton> getSingleton(byte[] singletonListColumnName, byte[] name, byte @Nullable[] defaultValue);

	Mono<? extends LLDictionary> getDictionary(byte[] columnName, UpdateMode updateMode);

	@Deprecated
	default Mono<? extends LLDictionary> getDeprecatedSet(String name, UpdateMode updateMode) {
		return getDictionary(ColumnUtils.deprecatedSet(name).name().getBytes(StandardCharsets.US_ASCII), updateMode);
	}

	default Mono<? extends LLDictionary> getDictionary(String name, UpdateMode updateMode) {
		return getDictionary(ColumnUtils.dictionary(name).name().getBytes(StandardCharsets.US_ASCII), updateMode);
	}

	default Mono<? extends LLSingleton> getSingleton(String singletonListName, String name) {
		return getSingleton(ColumnUtils.special(singletonListName).name().getBytes(StandardCharsets.US_ASCII),
				name.getBytes(StandardCharsets.US_ASCII),
				null
		);
	}

	default Mono<DatabaseInt> getInteger(String singletonListName, String name, int defaultValue) {
		return this
				.getSingleton(ColumnUtils.special(singletonListName).name().getBytes(StandardCharsets.US_ASCII),
						name.getBytes(StandardCharsets.US_ASCII),
						Ints.toByteArray(defaultValue)
				)
				.map(DatabaseInt::new);
	}

	default Mono<DatabaseLong> getLong(String singletonListName, String name, long defaultValue) {
		return this
				.getSingleton(ColumnUtils.special(singletonListName).name().getBytes(StandardCharsets.US_ASCII),
						name.getBytes(StandardCharsets.US_ASCII),
						Longs.toByteArray(defaultValue)
				)
				.map(DatabaseLong::new);
	}

	Mono<Void> verifyChecksum();

	Mono<Void> compact();

	Mono<Void> flush();

	BufferAllocator getAllocator();

	MeterRegistry getMeterRegistry();

	Mono<Void> preClose();
	Mono<Void> close();
}
