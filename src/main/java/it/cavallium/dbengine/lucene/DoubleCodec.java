package it.cavallium.dbengine.lucene;

import io.net5.buffer.ByteBuf;
import java.util.function.Function;

public class DoubleCodec implements LMDBCodec<Double> {

	@Override
	public ByteBuf serialize(Function<Integer, ByteBuf> allocator, Double data) {
		return allocator.apply(Double.BYTES).writeDouble(data);
	}

	@Override
	public Double deserialize(ByteBuf b) {
		return b.readDouble();
	}
}
