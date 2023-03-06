package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.serialization.Serializer;

public class SubStageGetterSingleBytes extends SubStageGetterSingle<Buf> {

	public SubStageGetterSingleBytes() {
		super(Serializer.NOOP_SERIALIZER);
	}
}
