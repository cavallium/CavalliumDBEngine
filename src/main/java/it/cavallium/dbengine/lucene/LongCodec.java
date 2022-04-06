package it.cavallium.dbengine.lucene;

import io.netty5.buffer.api.Buffer;
import java.util.function.Function;

public class LongCodec implements HugePqCodec<Long> {

	@Override
	public Buffer serialize(Function<Integer, Buffer> allocator, Long data) {
		return allocator.apply(Long.BYTES).writeLong(data);
	}

	@Override
	public Long deserialize(Buffer b) {
		return b.readLong();
	}

}
