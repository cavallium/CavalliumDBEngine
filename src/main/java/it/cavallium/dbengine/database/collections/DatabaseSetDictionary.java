package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseSetDictionary<T> extends DatabaseMapDictionary<T, Nothing> {

	protected DatabaseSetDictionary(LLDictionary dictionary,
			Send<Buffer> prefixKey,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			Drop<DatabaseMapDictionaryDeep<T, Nothing, DatabaseStageEntry<Nothing>>> drop) {
		super(dictionary, prefixKey, keySuffixSerializer, DatabaseEmpty.nothingSerializer(dictionary.getAllocator()), drop);
	}

	public static <T> DatabaseSetDictionary<T> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T> keySerializer,
			Drop<DatabaseMapDictionaryDeep<T, Nothing, DatabaseStageEntry<Nothing>>> drop) {
		return new DatabaseSetDictionary<>(dictionary, LLUtils.empty(dictionary.getAllocator()), keySerializer, drop);
	}

	public static <T> DatabaseSetDictionary<T> tail(LLDictionary dictionary,
			Send<Buffer> prefixKey,
			SerializerFixedBinaryLength<T> keySuffixSerializer,
			Drop<DatabaseMapDictionaryDeep<T, Nothing, DatabaseStageEntry<Nothing>>> drop) {
		return new DatabaseSetDictionary<>(dictionary, prefixKey, keySuffixSerializer, drop);
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
