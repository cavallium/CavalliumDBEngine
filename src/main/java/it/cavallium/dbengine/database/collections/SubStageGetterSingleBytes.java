package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.serialization.Serializer;

public class SubStageGetterSingleBytes extends SubStageGetterSingle<Send<Buffer>> {

	public SubStageGetterSingleBytes() {
		super(Serializer.NOOP_SEND_SERIALIZER);
	}
}
