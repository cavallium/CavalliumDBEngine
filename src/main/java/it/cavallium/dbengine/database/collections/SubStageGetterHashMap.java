package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Send;
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

	private final Serializer<T, Send<Buffer>> keySerializer;
	private final Serializer<U, Send<Buffer>> valueSerializer;
	private final Function<T, TH> keyHashFunction;
	private final SerializerFixedBinaryLength<TH, Send<Buffer>> keyHashSerializer;

	public SubStageGetterHashMap(Serializer<T, Send<Buffer>> keySerializer,
			Serializer<U, Send<Buffer>> valueSerializer,
			Function<T, TH> keyHashFunction,
			SerializerFixedBinaryLength<TH, Send<Buffer>> keyHashSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.keyHashFunction = keyHashFunction;
		this.keyHashSerializer = keyHashSerializer;
	}

	@Override
	public Mono<DatabaseMapDictionaryHashed<T, U, TH>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<Send<Buffer>> prefixKeyMono) {
		return Mono.usingWhen(
				prefixKeyMono,
				prefixKey -> Mono
						.fromSupplier(() -> DatabaseMapDictionaryHashed
								.tail(dictionary,
										prefixKey,
										keySerializer,
										valueSerializer,
										keyHashFunction,
										keyHashSerializer
								)
						),
				prefixKey -> Mono.fromRunnable(prefixKey::close)
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
