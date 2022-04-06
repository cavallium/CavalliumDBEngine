package it.cavallium.dbengine.lucene;

import io.netty5.buffer.api.Buffer;
import java.util.function.Function;

public class IntCodec implements HugePqCodec<Integer> {

	@Override
	public Buffer serialize(Function<Integer, Buffer> allocator, Integer data) {
		return allocator.apply(Integer.BYTES).writeInt(data);
	}

	@Override
	public Integer deserialize(Buffer b) {
		return b.readInt();
	}
}
