package it.cavallium.dbengine.database.serialization;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SafeCloseable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;

public class BufferDataInput implements DataInput, SafeCloseable {

	private final Buffer buf;

	public BufferDataInput(Send<Buffer> bufferSend) {
		this.buf = bufferSend.receive().makeReadOnly();
	}

	@Override
	public void readFully(byte @NotNull [] b) {
		this.readFully(b, 0, b.length);
	}

	@Override
	public void readFully(byte @NotNull [] b, int off, int len) {
		buf.copyInto(buf.readerOffset(), b, off, len);
		buf.readerOffset(buf.readerOffset() + len);
	}

	@Override
	public int skipBytes(int n) {
		n = Math.min(n, buf.readerOffset() - buf.writerOffset());
		buf.readerOffset(buf.readerOffset() + n);
		return n;
	}

	@Override
	public boolean readBoolean() {
		return buf.readUnsignedByte() != 0;
	}

	@Override
	public byte readByte() {
		return buf.readByte();
	}

	@Override
	public int readUnsignedByte() {
		return buf.readUnsignedByte();
	}

	@Override
	public short readShort() {
		return buf.readShort();
	}

	@Override
	public int readUnsignedShort() {
		return buf.readUnsignedShort();
	}

	@Override
	public char readChar() {
		return buf.readChar();
	}

	@Override
	public int readInt() {
		return buf.readInt();
	}

	@Override
	public long readLong() {
		return buf.readLong();
	}

	@Override
	public float readFloat() {
		return buf.readFloat();
	}

	@Override
	public double readDouble() {
		return buf.readDouble();
	}

	@Override
	public String readLine() {
		throw new UnsupportedOperationException();
	}

	@NotNull
	@Override
	public String readUTF() {
		var len = buf.readUnsignedShort();
		try (var copiedBuf = buf.copy(buf.readerOffset(), len)) {
			var off = copiedBuf.readerOffset();
			return LLUtils.deserializeString(copiedBuf.send(), off, len, StandardCharsets.UTF_8);
		}
	}

	@Override
	public void close() {
		buf.close();
	}
}
