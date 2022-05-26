package it.cavallium.dbengine.database.collections;

import com.google.common.primitives.Ints;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class DatabaseInt implements LLKeyValueDatabaseStructure {

	private final LLSingleton singleton;
	private final SerializerFixedBinaryLength<Integer> serializer;

	public DatabaseInt(LLSingleton singleton) {
		this.singleton = singleton;
		this.serializer = SerializerFixedBinaryLength.intSerializer(singleton.getAllocator());
	}

	public Mono<Integer> get(@Nullable LLSnapshot snapshot) {
		var resultMono = singleton.get(snapshot);
		return Mono.usingWhen(resultMono,
				result -> Mono.fromSupplier(() -> serializer.deserialize(result)),
				result -> Mono.fromRunnable(result::close)
		);
	}

	public Mono<Void> set(int value) {
		return singleton.set(Mono.fromCallable(() -> {
			var buf = singleton.getAllocator().allocate(Integer.BYTES);
			try {
				serializer.serialize(value, buf);
				return buf;
			} catch (Throwable ex) {
				buf.close();
				throw ex;
			}
		}));
	}

	@Override
	public String getDatabaseName() {
		return singleton.getDatabaseName();
	}
}
