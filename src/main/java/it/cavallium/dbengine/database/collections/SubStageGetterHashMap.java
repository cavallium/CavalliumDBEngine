package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Resource;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.Map;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class SubStageGetterHashMap<T, U, TH> implements
		SubStageGetter<Object2ObjectSortedMap<T, U>, DatabaseMapDictionaryHashed<T, U, TH>> {

	final Serializer<T> keySerializer;
	final Serializer<U> valueSerializer;
	final Function<T, TH> keyHashFunction;
	final SerializerFixedBinaryLength<TH> keyHashSerializer;

	public SubStageGetterHashMap(Serializer<T> keySerializer,
			Serializer<U> valueSerializer,
			Function<T, TH> keyHashFunction,
			SerializerFixedBinaryLength<TH> keyHashSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.keyHashFunction = keyHashFunction;
		this.keyHashSerializer = keyHashSerializer;
	}

	@Override
	public Mono<DatabaseMapDictionaryHashed<T, U, TH>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<Buffer> prefixKeyMono) {
		return prefixKeyMono.map(prefixKey -> DatabaseMapDictionaryHashed.tail(dictionary,
				BufSupplier.ofOwned(prefixKey),
				keySerializer,
				valueSerializer,
				keyHashFunction,
				keyHashSerializer,
				null
		));
	}

	public int getKeyHashBinaryLength() {
		return keyHashSerializer.getSerializedBinaryLength();
	}
}
