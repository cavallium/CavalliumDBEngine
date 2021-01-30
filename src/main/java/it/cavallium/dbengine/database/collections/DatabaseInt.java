package it.cavallium.dbengine.database.collections;

import com.google.common.primitives.Ints;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class DatabaseInt implements LLKeyValueDatabaseStructure {

	private final LLSingleton singleton;

	public DatabaseInt(LLSingleton singleton) {
		this.singleton = singleton;
	}

	public Mono<Integer> get(@Nullable LLSnapshot snapshot) {
		return singleton.get(snapshot).map(Ints::fromByteArray);
	}

	public Mono<Void> set(int value) {
		return singleton.set(Ints.toByteArray(value));
	}

	@Override
	public String getDatabaseName() {
		return singleton.getDatabaseName();
	}
}
