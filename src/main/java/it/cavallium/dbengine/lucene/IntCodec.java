package it.cavallium.dbengine.lucene;

import io.netty5.buffer.ByteBuf;
import java.util.function.Function;

public class IntCodec implements LMDBCodec<Integer> {

	@Override
	public ByteBuf serialize(Function<Integer, ByteBuf> allocator, Integer data) {
		return allocator.apply(Integer.BYTES).writeInt(data);
	}

	@Override
	public Integer deserialize(ByteBuf b) {
		return b.readInt();
	}
}
