package it.cavallium.dbengine.database.structures;

import com.google.common.primitives.Ints;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.IOException;
import org.jetbrains.annotations.Nullable;

public class LLInt implements LLKeyValueDatabaseStructure {

	private final LLSingleton singleton;

	public LLInt(LLSingleton singleton) {
		this.singleton = singleton;
	}

	public int get(@Nullable LLSnapshot snapshot) throws IOException {
		return Ints.fromByteArray(singleton.get(snapshot));
	}

	public void set(int value) throws IOException {
		singleton.set(Ints.toByteArray(value));
	}

	@Override
	public String getDatabaseName() {
		return singleton.getDatabaseName();
	}
}
