package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.client.IBackuppable;
import it.cavallium.dbengine.database.collections.DatabaseInt;
import it.cavallium.dbengine.database.collections.DatabaseLong;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;

public interface LLKeyValueDatabase extends LLSnapshottable, LLKeyValueDatabaseStructure, DatabaseProperties,
		IBackuppable, DatabaseOperations {

	LLSingleton getSingleton(byte[] singletonListColumnName, byte[] name, byte @Nullable [] defaultValue)
			throws IOException;

	LLDictionary getDictionary(byte[] columnName, UpdateMode updateMode);

	@Deprecated
	default LLDictionary getDeprecatedSet(String name, UpdateMode updateMode) {
		return getDictionary(ColumnUtils.deprecatedSet(name).name().getBytes(StandardCharsets.US_ASCII), updateMode);
	}

	default LLDictionary getDictionary(String name, UpdateMode updateMode) {
		return getDictionary(ColumnUtils.dictionary(name).name().getBytes(StandardCharsets.US_ASCII), updateMode);
	}

	default LLSingleton getSingleton(String singletonListName, String name) {
		return getSingleton(ColumnUtils.special(singletonListName).name().getBytes(StandardCharsets.US_ASCII),
				name.getBytes(StandardCharsets.US_ASCII),
				null
		);
	}

	default DatabaseInt getInteger(String singletonListName, String name, int defaultValue) {
		return new DatabaseInt(this.getSingleton(ColumnUtils
						.special(singletonListName)
						.name()
						.getBytes(StandardCharsets.US_ASCII),
				name.getBytes(StandardCharsets.US_ASCII),
				Ints.toByteArray(defaultValue)
		));
	}

	default DatabaseLong getLong(String singletonListName, String name, long defaultValue) {
		return new DatabaseLong(this.getSingleton(ColumnUtils
						.special(singletonListName)
						.name()
						.getBytes(StandardCharsets.US_ASCII),
				name.getBytes(StandardCharsets.US_ASCII),
				Longs.toByteArray(defaultValue)
		));
	}

	void verifyChecksum();

	void compact() throws RocksDBException;

	void flush();

	MeterRegistry getMeterRegistry();

	void preClose();

	void close();
}
