package it.cavallium.dbengine.database.serialization;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.SafeCloseable;
import java.io.DataInput;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BufferDataInputOwned implements SafeCloseable, BufferDataInput {

	@Nullable
	private final Buffer buf;
	private final int initialReaderOffset;

	public BufferDataInputOwned(@Nullable Send<Buffer> bufferSend) {
		this.buf = bufferSend == null ? null : bufferSend.receive().makeReadOnly();
		this.initialReaderOffset = buf == null ? 0 : buf.readerOffset();
	}

	@Override
	public void readFully(byte @NotNull [] b) {
		this.readFully(b, 0, b.length);
	}

	@Override
	public void readFully(byte @NotNull [] b, int off, int len) {
		if (buf == null) {
			if (len != 0) {
				throw new IndexOutOfBoundsException();
			}
		} else {
			buf.copyInto(buf.readerOffset(), b, off, len);
			buf.readerOffset(buf.readerOffset() + len);
		}
	}

	@Override
	public int skipBytes(int n) {
		if (buf == null) {
			if (n != 0) {
				throw new IndexOutOfBoundsException();
			}
			return 0;
		} else {
			n = Math.min(n, buf.readerOffset() - buf.writerOffset());
			buf.readerOffset(buf.readerOffset() + n);
			return n;
		}
	}

	@Override
	public boolean readBoolean() {
		if (buf == null) throw new IndexOutOfBoundsException();
		return buf.readUnsignedByte() != 0;
	}

	@Override
	public byte readByte() {
		if (buf == null) throw new IndexOutOfBoundsException();
		return buf.readByte();
	}

	@Override
	public int readUnsignedByte() {
		if (buf == null) throw new IndexOutOfBoundsException();
		return buf.readUnsignedByte();
	}

	@Override
	public short readShort() {
		if (buf == null) throw new IndexOutOfBoundsException();
		return buf.readShort();
	}

	@Override
	public int readUnsignedShort() {
		if (buf == null) throw new IndexOutOfBoundsException();
		return buf.readUnsignedShort();
	}

	@Override
	public char readChar() {
		if (buf == null) throw new IndexOutOfBoundsException();
		return buf.readChar();
	}

	@Override
	public int readInt() {
		if (buf == null) throw new IndexOutOfBoundsException();
		return buf.readInt();
	}

	@Override
	public long readLong() {
		if (buf == null) throw new IndexOutOfBoundsException();
		return buf.readLong();
	}

	@Override
	public float readFloat() {
		if (buf == null) throw new IndexOutOfBoundsException();
		return buf.readFloat();
	}

	@Override
	public double readDouble() {
		if (buf == null) throw new IndexOutOfBoundsException();
		return buf.readDouble();
	}

	@Override
	public String readLine() {
		if (buf == null) throw new IndexOutOfBoundsException();
		throw new UnsupportedOperationException();
	}

	@NotNull
	@Override
	public String readUTF() {
		if (buf == null) throw new IndexOutOfBoundsException();
		var len = buf.readUnsignedShort();
		byte[] bytes = new byte[len];
		buf.copyInto(buf.readerOffset(), bytes, 0, len);
		buf.readerOffset(buf.readerOffset() + len);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	@Override
	public void close() {
		if (buf != null) {
			buf.close();
		}
	}

	@Override
	public int getReadBytesCount() {
		if (buf == null) {
			return 0;
		} else {
			return buf.readerOffset() - initialReaderOffset;
		}
	}
}
