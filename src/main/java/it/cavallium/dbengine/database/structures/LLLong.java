package it.cavallium.dbengine.database.structures;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.IOException;
import org.jetbrains.annotations.Nullable;

public class LLLong implements LLKeyValueDatabaseStructure {

	private final LLSingleton singleton;

	public LLLong(LLSingleton singleton) {
		this.singleton = singleton;
	}

	public long get(@Nullable LLSnapshot snapshot) throws IOException {
		var array = singleton.get(snapshot);
		if (array.length == 4) {
			return Ints.fromByteArray(array);
		} else {
			return Longs.fromByteArray(array);
		}
	}

	public void set(long value) throws IOException {
		singleton.set(Longs.toByteArray(value));
	}

	@Override
	public String getDatabaseName() {
		return singleton.getDatabaseName();
	}
}
