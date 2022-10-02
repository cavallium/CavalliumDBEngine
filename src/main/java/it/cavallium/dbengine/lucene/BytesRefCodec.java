package it.cavallium.dbengine.lucene;

import io.netty5.buffer.Buffer;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;

public class BytesRefCodec implements HugePqCodec<BytesRef> {

	@Override
	public Buffer serialize(Function<Integer, Buffer> allocator, BytesRef data) {
		var buf = allocator.apply(data.length + Integer.BYTES);
		buf.writeInt(data.length);
		buf.writeBytes(data.bytes, data.offset, data.length);
		return buf;
	}

	@Override
	public BytesRef deserialize(Buffer b) {
		var length = b.readInt();
		var bytes = new byte[length];
		b.readBytes(bytes, 0, length);
		return new BytesRef(bytes, 0, length);
	}

	@Override
	public BytesRef clone(BytesRef obj) {
		return obj.clone();
	}
}
