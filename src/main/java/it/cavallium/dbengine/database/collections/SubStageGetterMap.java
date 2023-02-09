package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import org.jetbrains.annotations.Nullable;

public class SubStageGetterMap<T, U> implements
		SubStageGetter<Object2ObjectSortedMap<T, U>, DatabaseMapDictionary<T, U>> {

	final SerializerFixedBinaryLength<T> keySerializer;
	final Serializer<U> valueSerializer;

	public SubStageGetterMap(SerializerFixedBinaryLength<T> keySerializer,
			Serializer<U> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public DatabaseMapDictionary<T, U> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Buf prefixKey) {
		return DatabaseMapDictionary.tail(dictionary, prefixKey, keySerializer, valueSerializer);
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength();
	}
}
