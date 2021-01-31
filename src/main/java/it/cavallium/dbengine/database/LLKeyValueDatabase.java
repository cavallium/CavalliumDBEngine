package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.dbengine.database.collections.DatabaseInt;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import reactor.core.publisher.Mono;

public interface LLKeyValueDatabase extends Closeable, LLSnapshottable, LLKeyValueDatabaseStructure {

	Mono<? extends LLSingleton> getSingleton(byte[] singletonListColumnName, byte[] name, byte[] defaultValue);

	Mono<? extends LLDictionary> getDictionary(byte[] columnName);

	@Deprecated
	default Mono<? extends LLDictionary> getDeprecatedSet(String name) {
		return getDictionary(Column.deprecatedSet(name).getName().getBytes(StandardCharsets.US_ASCII));
	}

	default Mono<? extends LLDictionary> getDictionary(String name) {
		return getDictionary(Column.dictionary(name).getName().getBytes(StandardCharsets.US_ASCII));
	}

	default Mono<DatabaseInt> getInteger(String singletonListName, String name, int defaultValue) {
		return this
				.getSingleton(Column.special(singletonListName).getName().getBytes(StandardCharsets.US_ASCII),
						name.getBytes(StandardCharsets.US_ASCII),
						Ints.toByteArray(defaultValue)
				)
				.map(DatabaseInt::new);
	}

	default Mono<DatabaseInt> getLong(String singletonListName, String name, long defaultValue) {
		return this
				.getSingleton(Column.special(singletonListName).getName().getBytes(StandardCharsets.US_ASCII),
						name.getBytes(StandardCharsets.US_ASCII),
						Longs.toByteArray(defaultValue)
				)
				.map(DatabaseInt::new);
	}

	Mono<Long> getProperty(String propertyName);
}
