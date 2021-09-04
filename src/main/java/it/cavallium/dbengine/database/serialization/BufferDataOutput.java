package it.cavallium.dbengine.database.serialization;

import io.netty5.buffer.api.Buffer;
import java.io.DataOutput;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;

public class BufferDataOutput implements DataOutput {

	private final Buffer buf;

	public BufferDataOutput(Buffer bufferSend) {
		this.buf = bufferSend;
	}

	@Override
	public void write(int b) {
		buf.ensureWritable(Integer.BYTES);
		buf.writeUnsignedByte(b);
	}

	@Override
	public void write(byte @NotNull [] b) {
		buf.ensureWritable(Byte.BYTES * b.length);
		buf.writeBytes(b);
	}

	@Override
	public void write(byte @NotNull [] b, int off, int len) {
		buf.ensureWritable(len);
		buf.writeBytes(b, off, len);
	}

	@Override
	public void writeBoolean(boolean v) {
		buf.ensureWritable(Byte.BYTES);
		buf.writeUnsignedByte(v ? 1 : 0);
	}

	@Override
	public void writeByte(int v) {
		buf.ensureWritable(Byte.BYTES);
		buf.writeByte((byte) v);
	}

	@Override
	public void writeShort(int v) {
		buf.ensureWritable(Short.BYTES);
		buf.writeShort((short) v);
	}

	@Override
	public void writeChar(int v) {
		buf.ensureWritable(Character.BYTES);
		buf.writeChar((char) v);
	}

	@Override
	public void writeInt(int v) {
		buf.ensureWritable(Integer.BYTES);
		buf.writeInt(v);
	}

	@Override
	public void writeLong(long v) {
		buf.ensureWritable(Long.BYTES);
		buf.writeLong(v);
	}

	@Override
	public void writeFloat(float v) {
		buf.ensureWritable(Float.BYTES);
		buf.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) {
		buf.ensureWritable(Double.BYTES);
		buf.writeDouble(v);
	}

	@Override
	public void writeBytes(@NotNull String s) {
		var b= s.getBytes(StandardCharsets.UTF_8);
		buf.ensureWritable(Byte.BYTES * b.length);
		buf.writeBytes(b);
	}

	@Override
	public void writeChars(@NotNull String s) {
		var chars = s.toCharArray();
		buf.ensureWritable(Character.BYTES * chars.length);
		for (char c : chars) {
			buf.writeChar(c);
		}
	}

	@Override
	public void writeUTF(@NotNull String s) {
		var bytes = s.getBytes(StandardCharsets.UTF_8);
		buf.ensureWritable(Short.BYTES + Byte.BYTES * bytes.length);
		buf.writeUnsignedShort(bytes.length);
		buf.writeBytes(bytes);
	}
}
