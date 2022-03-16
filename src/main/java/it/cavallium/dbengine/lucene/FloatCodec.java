package it.cavallium.dbengine.lucene;

import io.netty5.buffer.ByteBuf;
import java.util.function.Function;

public class FloatCodec implements LMDBCodec<Float> {

	@Override
	public ByteBuf serialize(Function<Integer, ByteBuf> allocator, Float data) {
		return allocator.apply(Float.BYTES).writeFloat(data);
	}

	@Override
	public Float deserialize(ByteBuf b) {
		return b.readFloat();
	}
}
