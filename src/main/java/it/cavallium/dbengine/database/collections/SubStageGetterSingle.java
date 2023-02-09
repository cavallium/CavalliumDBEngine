package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import org.jetbrains.annotations.Nullable;

public class SubStageGetterSingle<T> implements SubStageGetter<T, DatabaseStageEntry<T>> {

	private final Serializer<T> serializer;

	public SubStageGetterSingle(Serializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	public DatabaseStageEntry<T> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Buf keyPrefix) {
		return new DatabaseMapSingle<>(dictionary, keyPrefix, serializer);
	}

}
