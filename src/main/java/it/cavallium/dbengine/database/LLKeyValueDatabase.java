package it.cavallium.dbengine.database;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.dbengine.database.structures.LLDeepMap;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import it.cavallium.dbengine.database.structures.LLFixedDeepSet;
import it.cavallium.dbengine.database.structures.LLInt;
import it.cavallium.dbengine.database.structures.LLLong;
import it.cavallium.dbengine.database.structures.LLMap;
import it.cavallium.dbengine.database.structures.LLSet;

public interface LLKeyValueDatabase extends Closeable, LLSnapshottable, LLKeyValueDatabaseStructure {

	LLSingleton getSingleton(byte[] singletonListColumnName, byte[] name, byte[] defaultValue)
			throws IOException;

	LLDictionary getDictionary(byte[] columnName) throws IOException;

	LLDeepDictionary getDeepDictionary(byte[] columnName, int keySize, int key2Size) throws IOException;

	default LLSet getSet(String name) throws IOException {
		LLDictionary dictionary = getDictionary(
				Column.fixedSet(name).getName().getBytes(StandardCharsets.US_ASCII));
		return new LLSet(dictionary);
	}

	default LLMap getMap(String name) throws IOException {
		LLDictionary dictionary = getDictionary(
				Column.hashMap(name).getName().getBytes(StandardCharsets.US_ASCII));
		return new LLMap(dictionary);
	}

	default LLFixedDeepSet getDeepSet(String name, int keySize, int key2Size) throws IOException {
		LLDeepDictionary deepDictionary = getDeepDictionary(
				Column.fixedSet(name).getName().getBytes(StandardCharsets.US_ASCII), keySize, key2Size);
		return new LLFixedDeepSet(deepDictionary);
	}

	default LLDeepMap getDeepMap(String name, int keySize, int key2Size) throws IOException {
		LLDeepDictionary deepDictionary = getDeepDictionary(
				Column.hashMap(name).getName().getBytes(StandardCharsets.US_ASCII), keySize, key2Size);
		return new LLDeepMap(deepDictionary);
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
