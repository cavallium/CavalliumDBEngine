package it.cavallium.dbengine.database.serialization;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SafeCloseable;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;

public class BufferDataOutput implements DataOutput {

	private final Buffer buf;

	public BufferDataOutput(Buffer bufferSend) {
		this.buf = bufferSend;
	}

	@Override
	public void write(int b) {
		buf.writeUnsignedByte(b);
	}

	@Override
	public void write(byte @NotNull [] b) {
		buf.writeBytes(b);
	}

	@Override
	public void write(byte @NotNull [] b, int off, int len) {
		buf.writeBytes(b, off, len);
	}

	@Override
	public void writeBoolean(boolean v) {
		buf.writeUnsignedByte(v ? 1 : 0);
	}

	@Override
	public void writeByte(int v) {
		buf.writeByte((byte) v);
	}

	@Override
	public void writeShort(int v) {
		buf.writeShort((short) v);
	}

	@Override
	public void writeChar(int v) {
		buf.writeChar((char) v);
	}

	@Override
	public void writeInt(int v) {
		buf.writeInt(v);
	}

	@Override
	public void writeLong(long v) {
		buf.writeLong(v);
	}

	@Override
	public void writeFloat(float v) {
		buf.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) {
		buf.writeDouble(v);
	}

	@Override
	public void writeBytes(@NotNull String s) {
		buf.writeBytes(s.getBytes());
	}

	@Override
	public void writeChars(@NotNull String s) {
		s.chars().forEach(c -> buf.writeChar((char) c));
	}

	@Override
	public void writeUTF(@NotNull String s) {
		var bytes = s.getBytes(StandardCharsets.UTF_8);
		buf.writeUnsignedShort(bytes.length);
		buf.writeBytes(bytes);
	}
}
