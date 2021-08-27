package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
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
public class DatabaseSetDictionary<T> extends DatabaseMapDictionary<T, Nothing> {

	protected DatabaseSetDictionary(LLDictionary dictionary,
			ByteBuf prefixKey,
			SerializerFixedBinaryLength<T, ByteBuf> keySuffixSerializer) {
		super(dictionary, prefixKey, keySuffixSerializer, DatabaseEmpty.NOTHING_SERIALIZER);
	}

	public static <T> DatabaseSetDictionary<T> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T, ByteBuf> keySerializer) {
		var buf = dictionary.getAllocator().buffer(0);
		try {
			return new DatabaseSetDictionary<>(dictionary, buf, keySerializer);
		} finally {
			buf.release();
		}
	}

	public static <T> DatabaseSetDictionary<T> tail(LLDictionary dictionary,
			ByteBuf prefixKey,
			SerializerFixedBinaryLength<T, ByteBuf> keySuffixSerializer) {
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
