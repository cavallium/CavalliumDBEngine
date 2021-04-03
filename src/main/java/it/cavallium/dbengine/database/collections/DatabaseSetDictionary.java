package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseSetDictionary<T> extends DatabaseMapDictionaryDeep<T, Nothing, DatabaseStageEntry<Nothing>> {

	protected DatabaseSetDictionary(LLDictionary dictionary,
			byte[] prefixKey,
			SerializerFixedBinaryLength<T, byte[]> keySuffixSerializer) {
		super(dictionary, prefixKey, keySuffixSerializer, DatabaseEmpty.createSubStageGetter(), 0);
	}

	public static <T> DatabaseSetDictionary<T> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, byte[]> keySerializer) {
		return new DatabaseSetDictionary<>(dictionary, EMPTY_BYTES, keySerializer);
	}

	public static <T> DatabaseSetDictionary<T> tail(LLDictionary dictionary,
			byte[] prefixKey,
			SerializerFixedBinaryLength<T, byte[]> keySuffixSerializer) {
		return new DatabaseSetDictionary<>(dictionary, prefixKey, keySuffixSerializer);
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
