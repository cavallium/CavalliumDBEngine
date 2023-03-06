package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import org.jetbrains.annotations.Nullable;

public interface SubStageGetter<U, US extends DatabaseStage<U>> {

	US subStage(LLDictionary dictionary, @Nullable CompositeSnapshot snapshot, Buf prefixKey);

}
