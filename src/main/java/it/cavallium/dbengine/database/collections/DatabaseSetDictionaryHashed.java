package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import java.util.Set;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public class DatabaseSetDictionaryHashed<T, TH> extends DatabaseMapDictionaryHashed<T, Nothing, TH> {

	protected DatabaseSetDictionaryHashed(LLDictionary dictionary,
			@Nullable Buf prefixKeySupplier,
			Serializer<T> keySuffixSerializer,
			Function<T, TH> keySuffixHashFunction,
			SerializerFixedBinaryLength<TH> keySuffixHashSerializer) {
		super(dictionary,
				prefixKeySupplier,
				keySuffixSerializer,
				DatabaseEmpty.nothingSerializer(),
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
			@Nullable Buf prefixKeySupplier,
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
