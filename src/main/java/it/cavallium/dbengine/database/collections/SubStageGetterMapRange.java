package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterMapRange implements SubStageGetter<Map<byte[], byte[]>, DatabaseStageEntry<Map<byte[], byte[]>>> {

	private final int keyLength;

	public SubStageGetterMapRange(int keyLength) {
		this.keyLength = keyLength;
	}

	@Override
	public Mono<DatabaseStageEntry<Map<byte[], byte[]>>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] prefixKey,
			Flux<byte[]> keyFlux) {
		return Mono.just(new DatabaseMapDictionaryRange(dictionary, prefixKey, keyLength));
	}
}
