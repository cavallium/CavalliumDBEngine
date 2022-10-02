package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.Drop;
import io.netty5.util.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseSetDictionaryHashed<T, TH> extends DatabaseMapDictionaryHashed<T, Nothing, TH> {

	protected DatabaseSetDictionaryHashed(LLDictionary dictionary,
			@Nullable BufSupplier prefixKeySupplier,
			Serializer<T> keySuffixSerializer,
			Function<T, TH> keySuffixHashFunction,
			SerializerFixedBinaryLength<TH> keySuffixHashSerializer) {
		super(dictionary,
				prefixKeySupplier,
				keySuffixSerializer,
				DatabaseEmpty.nothingSerializer(dictionary.getAllocator()),
				keySuffixHashFunction,
				keySuffixHashSerializer
		);
	}

	public static <T, TH> DatabaseSetDictionaryHashed<T, TH> simple(LLDictionary dictionary,
			Serializer<T> keySerializer,
			Function<T, TH> keyHashFunction,
			SerializerFixedBinaryLength<TH> keyHashSerializer) {
		return new DatabaseSetDictionaryHashed<>(dictionary,
				null,
				keySerializer,
				keyHashFunction,
				keyHashSerializer
		);
	}

	public static <T, TH> DatabaseSetDictionaryHashed<T, TH> tail(LLDictionary dictionary,
			@Nullable BufSupplier prefixKeySupplier,
			Serializer<T> keySuffixSerializer,
			Function<T, TH> keyHashFunction,
			SerializerFixedBinaryLength<TH> keyHashSerializer) {
		return new DatabaseSetDictionaryHashed<>(dictionary,
				prefixKeySupplier,
				keySuffixSerializer,
				keyHashFunction,
				keyHashSerializer
		);
	}

	public Mono<Set<T>> getKeySet(@Nullable CompositeSnapshot snapshot) {
		return get(snapshot).map(Map::keySet);
	}

	public Mono<Set<T>> setAndGetPreviousKeySet(Set<T> value) {
		var hm = new Object2ObjectLinkedOpenHashMap<T, Nothing>();
		for (T t : value) {
			hm.put(t, DatabaseEmpty.NOTHING);
		}
		return setAndGetPrevious(hm).map(Map::keySet);
	}

	public Mono<Set<T>> clearAndGetPreviousKeySet() {
		return clearAndGetPrevious().map(Map::keySet);
	}
}
