package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterSet<T> implements SubStageGetter<Map<T, Nothing>, DatabaseSetDictionary<T>> {

	private final SerializerFixedBinaryLength<T, Buffer> keySerializer;

	public SubStageGetterSet(SerializerFixedBinaryLength<T, Buffer> keySerializer) {
		this.keySerializer = keySerializer;
	}

	@Override
	public Mono<DatabaseSetDictionary<T>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<Buffer> prefixKeyMono) {
		return Mono.usingWhen(prefixKeyMono,
				prefixKey -> Mono
						.fromSupplier(() -> DatabaseSetDictionary.tail(dictionary, prefixKey.retain(), keySerializer)),
				prefixKey -> Mono.fromRunnable(prefixKey::release)
		);
	}

	@Override
	public boolean isMultiKey() {
		return true;
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength();
	}
}
