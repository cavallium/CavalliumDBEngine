package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.Drop;
import io.netty5.util.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseSetDictionary<T> extends DatabaseMapDictionary<T, Nothing> {

	protected DatabaseSetDictionary(LLDictionary dictionary,
			BufSupplier prefixKeySupplier,
			SerializerFixedBinaryLength<T> keySuffixSerializer) {
		super(dictionary,
				prefixKeySupplier,
				keySuffixSerializer,
				DatabaseEmpty.nothingSerializer(dictionary.getAllocator())
		);
	}

	public static <T> DatabaseSetDictionary<T> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T> keySerializer) {
		return new DatabaseSetDictionary<>(dictionary, null, keySerializer);
	}

	public static <T> DatabaseSetDictionary<T> tail(LLDictionary dictionary,
			BufSupplier prefixKeySupplier,
			SerializerFixedBinaryLength<T> keySuffixSerializer) {
		return new DatabaseSetDictionary<>(dictionary, prefixKeySupplier, keySuffixSerializer);
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
