package it.cavallium.dbengine.lucene;

import io.netty5.buffer.ByteBuf;
import java.util.function.Function;

public class ByteArrayCodec implements LMDBCodec<byte[]> {

	@Override
	public ByteBuf serialize(Function<Integer, ByteBuf> allocator, byte[] data) {
		var buf = allocator.apply(data.length + Integer.BYTES);
		buf.writeInt(data.length);
		buf.writeBytes(data);
		return buf;
	}

	@Override
	public byte[] deserialize(ByteBuf b) {
		var length = b.readInt();
		byte[] data = new byte[length];
		b.readBytes(data);
		return data;
	}
}
