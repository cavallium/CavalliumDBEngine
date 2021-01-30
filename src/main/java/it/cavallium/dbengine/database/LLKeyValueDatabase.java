package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.dbengine.database.collections.LLInt;
import it.cavallium.dbengine.database.collections.LLLong;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public interface LLKeyValueDatabase extends Closeable, LLSnapshottable, LLKeyValueDatabaseStructure {

	LLSingleton getSingleton(byte[] singletonListColumnName, byte[] name, byte[] defaultValue)
			throws IOException;

	LLDictionary getDictionary(byte[] columnName) throws IOException;

	default LLDictionary getSet(String name) throws IOException {
		return getDictionary(Column.fixedSet(name).getName().getBytes(StandardCharsets.US_ASCII));
	}

	default LLDictionary getMap(String name) throws IOException {
		return getDictionary(Column.hashMap(name).getName().getBytes(StandardCharsets.US_ASCII));
	}

	default LLInt getInteger(String singletonListName, String name, int defaultValue)
			throws IOException {
		LLSingleton singleton = getSingleton(
				Column.special(singletonListName).getName().getBytes(StandardCharsets.US_ASCII),
				name.getBytes(StandardCharsets.US_ASCII), Ints.toByteArray(defaultValue));
		return new LLInt(singleton);
	}

	default LLLong getLong(String singletonListName, String name, long defaultValue)
			throws IOException {
		LLSingleton singleton = getSingleton(
				Column.special(singletonListName).getName().getBytes(StandardCharsets.US_ASCII),
				name.getBytes(StandardCharsets.US_ASCII), Longs.toByteArray(defaultValue));
		return new LLLong(singleton);
	}

	long getProperty(String propertyName) throws IOException;
}
