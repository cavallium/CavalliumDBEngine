package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.serialization.Serializer;

public class SubStageGetterSingleBytes extends SubStageGetterSingle<Send<Buffer>> {

	public SubStageGetterSingleBytes() {
		super(Serializer.noop());
	}
}
