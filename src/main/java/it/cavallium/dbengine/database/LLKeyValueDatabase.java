package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.netty.buffer.api.BufferAllocator;
import it.cavallium.dbengine.database.collections.DatabaseInt;
import it.cavallium.dbengine.database.collections.DatabaseLong;
import java.nio.charset.StandardCharsets;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface LLKeyValueDatabase extends LLSnapshottable, LLKeyValueDatabaseStructure {

	Mono<? extends LLSingleton> getSingleton(byte[] singletonListColumnName, byte[] name, byte[] defaultValue);

	Mono<? extends LLDictionary> getDictionary(byte[] columnName, UpdateMode updateMode);

	@Deprecated
	default Mono<? extends LLDictionary> getDeprecatedSet(String name, UpdateMode updateMode) {
		return getDictionary(Column.deprecatedSet(name).name().getBytes(StandardCharsets.US_ASCII), updateMode);
	}

	default Mono<? extends LLDictionary> getDictionary(String name, UpdateMode updateMode) {
		return getDictionary(Column.dictionary(name).name().getBytes(StandardCharsets.US_ASCII), updateMode);
	}

	default Mono<DatabaseInt> getInteger(String singletonListName, String name, int defaultValue) {
		return this
				.getSingleton(Column.special(singletonListName).name().getBytes(StandardCharsets.US_ASCII),
						name.getBytes(StandardCharsets.US_ASCII),
						Ints.toByteArray(defaultValue)
				)
				.map(DatabaseInt::new);
	}

	default Mono<DatabaseLong> getLong(String singletonListName, String name, long defaultValue) {
		return this
				.getSingleton(Column.special(singletonListName).name().getBytes(StandardCharsets.US_ASCII),
						name.getBytes(StandardCharsets.US_ASCII),
						Longs.toByteArray(defaultValue)
				)
				.map(DatabaseLong::new);
	}

	Mono<Long> getProperty(String propertyName);

	Mono<Void> verifyChecksum();

	BufferAllocator getAllocator();

	Mono<Void> close();
}
