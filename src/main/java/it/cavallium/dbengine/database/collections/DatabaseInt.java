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
		return singleton.get(snapshot).handle((dataSend, sink) -> {
			try (var data = dataSend.receive()) {
				sink.next(serializer.deserialize(data));
			} catch (SerializationException e) {
				sink.error(e);
			}
		});
	}

	public Mono<Void> set(int value) {
		return singleton.set(Mono.fromCallable(() -> {
			try (var buf = singleton.getAllocator().allocate(Integer.BYTES)) {
				serializer.serialize(value, buf);
				return buf.send();
			}
		}));
	}

	@Override
	public String getDatabaseName() {
		return singleton.getDatabaseName();
	}
}
