package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
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
			Mono<Send<Buffer>> keyPrefixMono) {
		return Mono.usingWhen(
				keyPrefixMono,
				keyPrefix -> Mono
						.<DatabaseStageEntry<T>>fromSupplier(() -> new DatabaseSingle<>(dictionary, keyPrefix, serializer)),
				keyPrefix -> Mono.fromRunnable(keyPrefix::close)
		);
	}

}
