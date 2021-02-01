package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterMap<T, U> implements SubStageGetter<Map<T, U>, DatabaseStageEntry<Map<T, U>>> {

	private final SerializerFixedBinaryLength<T, byte[]> keySerializer;
	private final Serializer<U, byte[]> valueSerializer;

	public SubStageGetterMap(SerializerFixedBinaryLength<T, byte[]> keySerializer,
			Serializer<U, byte[]> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public Mono<DatabaseStageEntry<Map<T, U>>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] prefixKey,
			Flux<byte[]> keyFlux) {
		return Mono.just(DatabaseMapDictionary.tail(dictionary, keySerializer, valueSerializer, prefixKey));
	}
}
