package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import java.util.Arrays;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterSingle implements SubStageGetter<byte[], DatabaseStageEntry<byte[]>> {

	@Override
	public Mono<DatabaseStageEntry<byte[]>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] keyPrefix,
			Flux<byte[]> keyFlux) {
		return keyFlux.singleOrEmpty().flatMap(key -> Mono.fromCallable(() -> {
			if (!Arrays.equals(keyPrefix, key)) {
				throw new IndexOutOfBoundsException("Found more than one element!");
			}
			return null;
		})).thenReturn(new DatabaseSingle(dictionary, keyPrefix));
	}
}
