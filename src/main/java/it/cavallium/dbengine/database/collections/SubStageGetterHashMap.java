package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class SubStageGetterHashMap<T, U, TH> implements
		SubStageGetter<Map<T, U>, DatabaseMapDictionaryHashed<T, U, TH>> {

	private final Serializer<T, Buffer> keySerializer;
	private final Serializer<U, Buffer> valueSerializer;
	private final Function<T, TH> keyHashFunction;
	private final SerializerFixedBinaryLength<TH, Buffer> keyHashSerializer;

	public SubStageGetterHashMap(Serializer<T, Buffer> keySerializer,
			Serializer<U, Buffer> valueSerializer,
			Function<T, TH> keyHashFunction,
			SerializerFixedBinaryLength<TH, Buffer> keyHashSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.keyHashFunction = keyHashFunction;
		this.keyHashSerializer = keyHashSerializer;
	}

	@Override
	public Mono<DatabaseMapDictionaryHashed<T, U, TH>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<Buffer> prefixKeyMono) {
		return Mono.usingWhen(
				prefixKeyMono,
				prefixKey -> Mono
						.fromSupplier(() -> DatabaseMapDictionaryHashed
								.tail(dictionary,
										prefixKey.retain(),
										keySerializer,
										valueSerializer,
										keyHashFunction,
										keyHashSerializer
								)
						),
				prefixKey -> Mono.fromRunnable(prefixKey::release)
		);
	}

	@Override
	public boolean isMultiKey() {
		return true;
	}

	public int getKeyHashBinaryLength() {
		return keyHashSerializer.getSerializedBinaryLength();
	}
}
