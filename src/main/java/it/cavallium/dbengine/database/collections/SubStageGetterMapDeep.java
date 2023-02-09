package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import org.jetbrains.annotations.Nullable;

public class SubStageGetterMapDeep<T, U, US extends DatabaseStage<U>> implements
		SubStageGetter<Object2ObjectSortedMap<T, U>, DatabaseMapDictionaryDeep<T, U, US>> {

	private final SubStageGetter<U, US> subStageGetter;
	private final SerializerFixedBinaryLength<T> keySerializer;
	private final int keyExtLength;

	public SubStageGetterMapDeep(SubStageGetter<U, US> subStageGetter,
			SerializerFixedBinaryLength<T> keySerializer,
			int keyExtLength) {
		this.subStageGetter = subStageGetter;
		this.keySerializer = keySerializer;
		this.keyExtLength = keyExtLength;
		assert keyExtConsistency();
	}


	private boolean keyExtConsistency() {
		if (subStageGetter instanceof SubStageGetterMapDeep) {
			return keyExtLength == ((SubStageGetterMapDeep<?, ?, ?>) subStageGetter).getKeyBinaryLength();
		} else if (subStageGetter instanceof SubStageGetterMap) {
			return keyExtLength == ((SubStageGetterMap<?, ?>) subStageGetter).getKeyBinaryLength();
		} else {
			return true;
		}
	}

	@Override
	public DatabaseMapDictionaryDeep<T, U, US> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Buf prefixKey) {
		return DatabaseMapDictionaryDeep.deepIntermediate(dictionary,
				prefixKey,
				keySerializer,
				subStageGetter,
				keyExtLength
		);
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength() + keyExtLength;
	}
}
