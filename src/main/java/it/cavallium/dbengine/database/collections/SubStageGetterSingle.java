package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import java.util.Arrays;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterSingle<T> implements SubStageGetter<T, DatabaseStageEntry<T>> {

	private final Serializer<T, byte[]> serializer;

	public SubStageGetterSingle(Serializer<T, byte[]> serializer) {
		this.serializer = serializer;
	}

	@Override
	public Mono<DatabaseStageEntry<T>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] keyPrefix,
			Flux<byte[]> keyFlux) {
		return keyFlux.singleOrEmpty().flatMap(key -> Mono.fromCallable(() -> {
			if (!Arrays.equals(keyPrefix, key)) {
				throw new IndexOutOfBoundsException("Found more than one element!");
			}
			return null;
		})).thenReturn(new DatabaseSingle<>(dictionary, keyPrefix, serializer));
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private T deserialize(byte[] bytes) {
		return serializer.deserialize(bytes);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private byte[] serialize(T bytes) {
		return serializer.serialize(bytes);
	}
}
