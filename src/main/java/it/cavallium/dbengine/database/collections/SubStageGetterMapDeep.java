package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterMapDeep<U, US extends DatabaseStage<U>> implements
		SubStageGetter<Map<byte[], U>, DatabaseStageEntry<Map<byte[], U>>> {

	private final SubStageGetter<U, US> subStageGetter;
	private final int keyLength;
	private final int keyExtLength;

	public SubStageGetterMapDeep(SubStageGetter<U, US> subStageGetter, int keyLength, int keyExtLength) {
		this.subStageGetter = subStageGetter;
		this.keyLength = keyLength;
		this.keyExtLength = keyExtLength;
	}

	@Override
	public Mono<DatabaseStageEntry<Map<byte[], U>>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] prefixKey,
			Flux<byte[]> keyFlux) {
		return Mono.just(new DatabaseMapDictionaryParent<>(dictionary, subStageGetter, prefixKey, keyLength, keyExtLength));
	}
}
