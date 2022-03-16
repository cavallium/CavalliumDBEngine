package it.cavallium.dbengine.lucene;

import io.netty5.buffer.ByteBuf;
import java.util.function.Function;

public class LongCodec implements LMDBCodec<Long> {

	@Override
	public ByteBuf serialize(Function<Integer, ByteBuf> allocator, Long data) {
		return allocator.apply(Long.BYTES).writeLong(data);
	}

	@Override
	public Long deserialize(ByteBuf b) {
		return b.readLong();
	}
}
