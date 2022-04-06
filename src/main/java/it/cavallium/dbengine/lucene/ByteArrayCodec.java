package it.cavallium.dbengine.lucene;

import io.netty5.buffer.api.Buffer;
import java.util.function.Function;

public class ByteArrayCodec implements HugePqCodec<byte[]> {

	@Override
	public Buffer serialize(Function<Integer, Buffer> allocator, byte[] data) {
		var buf = allocator.apply(data.length + Integer.BYTES);
		buf.writeInt(data.length);
		buf.writeBytes(data);
		return buf;
	}

	@Override
	public byte[] deserialize(Buffer b) {
		var length = b.readInt();
		byte[] data = new byte[length];
		b.readBytes(data, 0, length);
		return data;
	}

	@Override
	public byte[] clone(byte[] obj) {
		return obj.clone();
	}
}
