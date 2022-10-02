package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Buffer;
import io.netty5.util.Resource;
import io.netty5.util.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.Map;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

@SuppressWarnings({"unused", "ClassCanBeRecord"})
public class SubStageGetterHashSet<T, TH> implements
		SubStageGetter<Object2ObjectSortedMap<T, Nothing>, DatabaseSetDictionaryHashed<T, TH>> {

	final Serializer<T> keySerializer;
	final Function<T, TH> keyHashFunction;
	final SerializerFixedBinaryLength<TH> keyHashSerializer;

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
			Mono<Buffer> prefixKeyMono) {
		return prefixKeyMono.map(prefixKey -> DatabaseSetDictionaryHashed.tail(dictionary,
				BufSupplier.ofOwned(prefixKey),
				keySerializer,
				keyHashFunction,
				keyHashSerializer
		));
	}

	public int getKeyHashBinaryLength() {
		return keyHashSerializer.getSerializedBinaryLength();
	}
}
