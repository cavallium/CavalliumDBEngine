package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Buffer;
import io.netty5.util.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class SubStageGetterSingle<T> implements SubStageGetter<T, DatabaseStageEntry<T>> {

	private final Serializer<T> serializer;

	public SubStageGetterSingle(Serializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	public Mono<DatabaseStageEntry<T>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<Buffer> keyPrefixMono) {
		return keyPrefixMono.map(keyPrefix -> new DatabaseMapSingle<>(dictionary,
				BufSupplier.ofOwned(keyPrefix),
				serializer
		));
	}

}
