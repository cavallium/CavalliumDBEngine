package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterMap<T, U> implements SubStageGetter<Map<T, U>, DatabaseMapDictionary<T, U>> {

	private final SerializerFixedBinaryLength<T, ByteBuf> keySerializer;
	private final Serializer<U, ByteBuf> valueSerializer;

	public SubStageGetterMap(SerializerFixedBinaryLength<T, ByteBuf> keySerializer,
			Serializer<U, ByteBuf> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public Mono<DatabaseMapDictionary<T, U>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<ByteBuf> prefixKeyMono) {
		return Mono.usingWhen(prefixKeyMono,
				prefixKey -> Mono
						.fromSupplier(() -> DatabaseMapDictionary
								.tail(dictionary,
										prefixKey.retain(),
										keySerializer,
										valueSerializer
								)
						),
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
