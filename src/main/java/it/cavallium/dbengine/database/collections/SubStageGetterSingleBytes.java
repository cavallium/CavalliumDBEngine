package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.database.serialization.Serializer;

public class SubStageGetterSingleBytes extends SubStageGetterSingle<byte[]> {

	public SubStageGetterSingleBytes() {
		super(Serializer.noop());
	}
}
