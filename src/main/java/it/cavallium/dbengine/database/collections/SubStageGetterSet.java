package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import org.jetbrains.annotations.Nullable;

public class SubStageGetterSet<T> implements
		SubStageGetter<Object2ObjectSortedMap<T, Nothing>, DatabaseSetDictionary<T>> {

	private final SerializerFixedBinaryLength<T> keySerializer;

	public SubStageGetterSet(SerializerFixedBinaryLength<T> keySerializer) {
		this.keySerializer = keySerializer;
	}

	@Override
	public DatabaseSetDictionary<T> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Buf prefixKey) {
		return DatabaseSetDictionary.tail(dictionary, prefixKey, keySerializer);
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength();
	}
}
