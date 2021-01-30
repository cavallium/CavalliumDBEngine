package it.cavallium.dbengine.database.structures;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class LLLong implements LLKeyValueDatabaseStructure {

	private final LLSingleton singleton;

	public LLLong(LLSingleton singleton) {
		this.singleton = singleton;
	}

	public Mono<Long> get(@Nullable LLSnapshot snapshot) {
		return singleton.get(snapshot).map(array -> {
			if (array.length == 4) {
				return (long) Ints.fromByteArray(array);
			} else {
				return Longs.fromByteArray(array);
			}
		});
	}

	public Mono<Void> set(long value) {
		return singleton.set(Longs.toByteArray(value));
	}

	@Override
	public String getDatabaseName() {
		return singleton.getDatabaseName();
	}
}
