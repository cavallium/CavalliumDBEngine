package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public class SubStageGetterHashMap<T, U, TH> implements
		SubStageGetter<Object2ObjectSortedMap<T, U>, DatabaseMapDictionaryHashed<T, U, TH>> {

	final Serializer<T> keySerializer;
	final Serializer<U> valueSerializer;
	final Function<T, TH> keyHashFunction;
	final SerializerFixedBinaryLength<TH> keyHashSerializer;

	public SubStageGetterHashMap(Serializer<T> keySerializer,
			Serializer<U> valueSerializer,
			Function<T, TH> keyHashFunction,
			SerializerFixedBinaryLength<TH> keyHashSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.keyHashFunction = keyHashFunction;
		this.keyHashSerializer = keyHashSerializer;
	}

	@Override
	public DatabaseMapDictionaryHashed<T, U, TH> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Buf prefixKey) {
		return DatabaseMapDictionaryHashed.tail(dictionary,
				prefixKey,
				keySerializer,
				valueSerializer,
				keyHashFunction,
				keyHashSerializer
		);
	}

	public int getKeyHashBinaryLength() {
		return keyHashSerializer.getSerializedBinaryLength();
	}
}
