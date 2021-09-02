package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Map;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings({"unused", "ClassCanBeRecord"})
public class SubStageGetterHashSet<T, TH> implements
		SubStageGetter<Map<T, Nothing>, DatabaseSetDictionaryHashed<T, TH>> {

	private final Serializer<T> keySerializer;
	private final Function<T, TH> keyHashFunction;
	private final SerializerFixedBinaryLength<TH> keyHashSerializer;

	public SubStageGetterHashSet(Serializer<T> keySerializer,
			Function<T, TH> keyHashFunction,
			SerializerFixedBinaryLength<TH> keyHashSerializer) {
		this.keySerializer = keySerializer;
		this.keyHashFunction = keyHashFunction;
		this.keyHashSerializer = keyHashSerializer;
	}

	@Override
	public Mono<DatabaseSetDictionaryHashed<T, TH>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<Send<Buffer>> prefixKeyMono) {
		return Mono.usingWhen(prefixKeyMono,
				prefixKey -> Mono
						.fromSupplier(() -> DatabaseSetDictionaryHashed
								.tail(dictionary,
										prefixKey,
										keySerializer,
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
