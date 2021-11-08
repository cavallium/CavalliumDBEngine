package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Resource;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class SubStageGetterMap<T, U> implements SubStageGetter<Map<T, U>, DatabaseMapDictionary<T, U>> {

	private final SerializerFixedBinaryLength<T> keySerializer;
	private final Serializer<U> valueSerializer;

	public SubStageGetterMap(SerializerFixedBinaryLength<T> keySerializer,
			Serializer<U> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public Mono<DatabaseMapDictionary<T, U>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<Send<Buffer>> prefixKeyMono) {
		return prefixKeyMono.map(prefixKeyToReceive -> {
			var prefixKey = prefixKeyToReceive.receive();
			return DatabaseMapDictionary.tail(dictionary, prefixKey, keySerializer, valueSerializer, null);
		}).doOnDiscard(Send.class, Send::close).doOnDiscard(Resource.class, Resource::close);
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength();
	}
}
