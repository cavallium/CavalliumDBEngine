package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Buffer;
import io.netty5.util.Send;
import it.cavallium.dbengine.database.serialization.Serializer;

public class SubStageGetterSingleBytes extends SubStageGetterSingle<Send<Buffer>> {

	public SubStageGetterSingleBytes() {
		super(Serializer.NOOP_SEND_SERIALIZER);
	}
}
