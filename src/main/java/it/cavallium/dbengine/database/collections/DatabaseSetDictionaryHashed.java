package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseSetDictionaryHashed<T, TH> extends DatabaseMapDictionaryHashed<T, Nothing, TH> {

	protected DatabaseSetDictionaryHashed(LLDictionary dictionary,
			Send<Buffer> prefixKey,
			Serializer<T> keySuffixSerializer,
			Function<T, TH> keySuffixHashFunction,
			SerializerFixedBinaryLength<TH> keySuffixHashSerializer) {
		super(dictionary,
				prefixKey,
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
				dictionary.getAllocator().allocate(0).send(),
				keySerializer,
				keyHashFunction,
				keyHashSerializer
		);
	}

	public static <T, TH> DatabaseSetDictionaryHashed<T, TH> tail(LLDictionary dictionary,
			Send<Buffer> prefixKey,
			Serializer<T> keySuffixSerializer,
			Function<T, TH> keyHashFunction,
			SerializerFixedBinaryLength<TH> keyHashSerializer) {
		return new DatabaseSetDictionaryHashed<>(dictionary,
				prefixKey,
				keySuffixSerializer,
				keyHashFunction,
				keyHashSerializer
		);
	}

	public Mono<Set<T>> getKeySet(@Nullable CompositeSnapshot snapshot) {
		return get(snapshot).map(Map::keySet);
	}

	public Mono<Set<T>> setAndGetPreviousKeySet(Set<T> value) {
		var hm = new HashMap<T, Nothing>();
		for (T t : value) {
			hm.put(t, DatabaseEmpty.NOTHING);
		}
		return setAndGetPrevious(hm).map(Map::keySet);
	}

	public Mono<Set<T>> clearAndGetPreviousKeySet() {
		return clearAndGetPrevious().map(Map::keySet);
	}
}
