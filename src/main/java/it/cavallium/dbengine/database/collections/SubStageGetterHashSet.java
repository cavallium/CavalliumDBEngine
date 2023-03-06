package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"unused"})
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
	public DatabaseSetDictionaryHashed<T, TH> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Buf prefixKey) {
		return DatabaseSetDictionaryHashed.tail(dictionary,
				prefixKey,
				keySerializer,
				keyHashFunction,
				keyHashSerializer
		);
	}

	public int getKeyHashBinaryLength() {
		return keyHashSerializer.getSerializedBinaryLength();
	}
}
