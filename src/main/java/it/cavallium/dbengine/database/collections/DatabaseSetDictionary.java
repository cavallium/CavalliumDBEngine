package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import java.util.Set;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public class DatabaseSetDictionary<T> extends DatabaseMapDictionary<T, Nothing> {

	protected DatabaseSetDictionary(LLDictionary dictionary,
			Buf prefixKeySupplier,
			SerializerFixedBinaryLength<T> keySuffixSerializer) {
		super(dictionary, prefixKeySupplier, keySuffixSerializer, DatabaseEmpty.nothingSerializer());
	}

	public static <T> DatabaseSetDictionary<T> simple(LLDictionary dictionary,
			SerializerFixedBinaryLength<T> keySerializer) {
		return new DatabaseSetDictionary<>(dictionary, null, keySerializer);
	}

	public static <T> DatabaseSetDictionary<T> tail(LLDictionary dictionary,
			Buf prefixKeySupplier,
			SerializerFixedBinaryLength<T> keySuffixSerializer) {
		return new DatabaseSetDictionary<>(dictionary, prefixKeySupplier, keySuffixSerializer);
	}

	public Set<T> getKeySet(@Nullable CompositeSnapshot snapshot) {
		var v = get(snapshot);
		return v != null ? v.keySet() : null;
	}

	public Set<T> setAndGetPreviousKeySet(Set<T> value) {
		var hm = new Object2ObjectLinkedOpenHashMap<T, Nothing>();
		for (T t : value) {
			hm.put(t, DatabaseEmpty.NOTHING);
		}
		var v = setAndGetPrevious(hm);
		return v != null ? v.keySet() : null;
	}

	public Set<T> clearAndGetPreviousKeySet() {
		var v = clearAndGetPrevious();
		return v != null ? v.keySet() : null;
	}
}
