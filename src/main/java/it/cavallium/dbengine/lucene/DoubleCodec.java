package it.cavallium.dbengine.lucene;

import io.netty5.buffer.api.Buffer;
import java.util.function.Function;

public class DoubleCodec implements HugePqCodec<Double> {

	@Override
	public Buffer serialize(Function<Integer, Buffer> allocator, Double data) {
		return allocator.apply(Double.BYTES).writeDouble(data);
	}

	@Override
	public Double deserialize(Buffer b) {
		return b.readDouble();
	}
}
