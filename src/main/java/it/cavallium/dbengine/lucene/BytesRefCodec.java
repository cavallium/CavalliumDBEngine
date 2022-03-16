package it.cavallium.dbengine.lucene;

import io.netty5.buffer.ByteBuf;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;

public class BytesRefCodec implements LMDBCodec<BytesRef> {

	@Override
	public ByteBuf serialize(Function<Integer, ByteBuf> allocator, BytesRef data) {
		var buf = allocator.apply(data.length + Integer.BYTES);
		buf.writeInt(data.length);
		buf.writeBytes(data.bytes, data.offset, data.length);
		return buf;
	}

	@Override
	public BytesRef deserialize(ByteBuf b) {
		var length = b.readInt();
		var bytes = new byte[length];
		b.readBytes(bytes, 0, length);
		return new BytesRef(bytes, 0, length);
	}
}
