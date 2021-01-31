package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterMapDeep<T, U, US extends DatabaseStage<U>> implements
		SubStageGetter<Map<T, U>, DatabaseStageEntry<Map<T, U>>> {

	private final SubStageGetter<U, US> subStageGetter;
	private final FixedLengthSerializer<T> keySerializer;
	private final int keyLength;
	private final int keyExtLength;

	public SubStageGetterMapDeep(SubStageGetter<U, US> subStageGetter,
			FixedLengthSerializer<T> keySerializer,
			int keyLength,
			int keyExtLength) {
		this.subStageGetter = subStageGetter;
		this.keySerializer = keySerializer;
		this.keyLength = keyLength;
		this.keyExtLength = keyExtLength;
	}

	@Override
	public Mono<DatabaseStageEntry<Map<T, U>>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] prefixKey,
			Flux<byte[]> keyFlux) {
		return Mono.just(new DatabaseMapDictionary<>(dictionary,
				subStageGetter,
				keySerializer,
				prefixKey,
				keyLength,
				keyExtLength
		));
	}
}
