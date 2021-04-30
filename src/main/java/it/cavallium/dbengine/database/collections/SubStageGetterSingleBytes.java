package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import it.cavallium.dbengine.database.serialization.Serializer;

public class SubStageGetterSingleBytes extends SubStageGetterSingle<ByteBuf> {

	public SubStageGetterSingleBytes() {
		super(Serializer.noop());
	}
}
