package it.cavallium.dbengine.lucene;

import io.netty5.buffer.ByteBuf;
import io.netty5.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.util.function.Function;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

public class SortFieldCodec implements LMDBCodec<SortField> {

	@Override
	public ByteBuf serialize(Function<Integer, ByteBuf> allocator, SortField data) {
		var out = new ByteBufDataOutput();
		try {
			var provider = data.getIndexSorter().getProviderName();
			out.writeString(provider);
			SortField.Provider.forName(provider).writeSortField(data, out);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return out.buf;
	}

	@Override
	public SortField deserialize(ByteBuf b) {
		var in = new ByteBufDataInput(b);
		try {
			return SortField.Provider.forName(in.readString()).readSortField(in);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static class ByteBufDataOutput extends DataOutput {

		private final ByteBuf buf;

		public ByteBufDataOutput() {
			this.buf = PooledByteBufAllocator.DEFAULT.directBuffer();
		}

		@Override
		public void writeByte(byte b) {
			buf.writeByte(b);
		}

		@Override
		public void writeBytes(byte[] b, int offset, int length) throws IOException {
			buf.writeBytes(b, offset, length);
		}
	}

	private static class ByteBufDataInput extends DataInput {

		private final ByteBuf buf;

		public ByteBufDataInput(ByteBuf b) {
			this.buf = b;
		}

		@Override
		public byte readByte() {
			return buf.readByte();
		}

		@Override
		public void readBytes(byte[] b, int offset, int len) {
			buf.readBytes(b, offset, len);
		}

		@Override
		public void skipBytes(long numBytes) {
			buf.skipBytes((int) numBytes);
		}
	}
}
