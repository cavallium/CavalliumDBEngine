package it.cavallium.dbengine.database;

import java.io.IOException;
import org.jetbrains.annotations.Nullable;

public interface LLSingleton extends LLKeyValueDatabaseStructure {

	byte[] get(@Nullable LLSnapshot snapshot) throws IOException;

	void set(byte[] value) throws IOException;
}
