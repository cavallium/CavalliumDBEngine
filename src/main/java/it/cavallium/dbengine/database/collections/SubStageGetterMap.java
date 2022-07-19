package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.api.Buffer;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class SubStageGetterMap<T, U> implements
		SubStageGetter<Object2ObjectSortedMap<T, U>, DatabaseMapDictionary<T, U>> {

	final SerializerFixedBinaryLength<T> keySerializer;
	final Serializer<U> valueSerializer;

	public SubStageGetterMap(SerializerFixedBinaryLength<T> keySerializer,
			Serializer<U> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public Mono<DatabaseMapDictionary<T, U>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<Buffer> prefixKeyMono) {
		return prefixKeyMono.map(prefixKey -> DatabaseMapDictionary.tail(dictionary,
				BufSupplier.ofOwned(prefixKey),
				keySerializer,
				valueSerializer
		));
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength();
	}
}
