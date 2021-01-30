package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterSingle implements SubStageGetter<byte[], DatabaseStageEntry<byte[]>> {

	@Override
	public Mono<DatabaseStageEntry<byte[]>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] keyPrefix,
			Flux<byte[]> keyFlux) {
		return keyFlux.single().map(key -> new DatabaseSingle(dictionary, key));
	}
}
