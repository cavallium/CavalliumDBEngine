package it.cavallium.dbengine.lucene;

import io.netty5.buffer.Buffer;
import java.util.function.Function;

public class FloatCodec implements HugePqCodec<Float> {

	@Override
	public Buffer serialize(Function<Integer, Buffer> allocator, Float data) {
		return allocator.apply(Float.BYTES).writeFloat(data);
	}

	@Override
	public Float deserialize(Buffer b) {
		return b.readFloat();
	}
}
