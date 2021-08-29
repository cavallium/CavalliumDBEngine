package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import it.cavallium.dbengine.database.serialization.Serializer;

public class SubStageGetterSingleBytes extends SubStageGetterSingle<Buffer> {

	public SubStageGetterSingleBytes() {
		super(Serializer.noop());
	}
}
