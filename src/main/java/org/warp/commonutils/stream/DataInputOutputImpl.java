package org.warp.commonutils.stream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class DataInputOutputImpl implements DataInputOutput {

	private final DataInput in;
	private final DataOutput out;

	public DataInputOutputImpl(DataInput in, DataOutput out) {
		this.in = in;
		this.out = out;
	}

	@Override
	public DataInput getIn() {
		return this;
	}

	@Override
	public DataOutput getOut() {
		return this;
	}

	@Override
	public void readFully(byte @NotNull [] bytes) throws IOException {
		in.readFully(bytes);
	}

	@Override
	public void readFully(byte @NotNull [] bytes, int i, int i1) throws IOException {
		in.readFully(bytes, i, i1);
	}

	@Override
	public int skipBytes(int i) throws IOException {
		return in.skipBytes(i);
	}

	@Override
	public boolean readBoolean() throws IOException {
		return in.readBoolean();
	}

	@Override
	public byte readByte() throws IOException {
		return in.readByte();
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return in.readUnsignedByte();
	}

	@Override
	public short readShort() throws IOException {
		return in.readShort();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return in.readUnsignedShort();
	}

	@Override
	public char readChar() throws IOException {
		return in.readChar();
	}

	@Override
	public int readInt() throws IOException {
		return in.readInt();
	}

	@Override
	public long readLong() throws IOException {
		return in.readLong();
	}

	@Override
	public float readFloat() throws IOException {
		return in.readFloat();
	}

	@Override
	public double readDouble() throws IOException {
		return in.readDouble();
	}

	@Override
	public String readLine() throws IOException {
		return in.readLine();
	}

	@NotNull
	@Override
	public String readUTF() throws IOException {
		return in.readUTF();
	}

	@Override
	public void write(int i) throws IOException {
		out.write(i);
	}

	@Override
	public void write(byte @NotNull [] bytes) throws IOException {
		out.write(bytes);
	}

	@Override
	public void write(byte @NotNull [] bytes, int i, int i1) throws IOException {
		out.write(bytes, i, i1);
	}

	@Override
	public void writeBoolean(boolean b) throws IOException {
		out.writeBoolean(b);
	}

	@Override
	public void writeByte(int i) throws IOException {
		out.writeByte(i);
	}

	@Override
	public void writeShort(int i) throws IOException {
		out.writeShort(i);
	}

	@Override
	public void writeChar(int i) throws IOException {
		out.writeChar(i);
	}

	@Override
	public void writeInt(int i) throws IOException {
		out.writeInt(i);
	}

	@Override
	public void writeLong(long l) throws IOException {
		out.writeLong(l);
	}

	@Override
	public void writeFloat(float v) throws IOException {
		out.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) throws IOException {
		out.writeDouble(v);
	}

	@Override
	public void writeBytes(@NotNull String s) throws IOException {
		out.writeBytes(s);
	}

	@Override
	public void writeChars(@NotNull String s) throws IOException {
		out.writeChars(s);
	}

	@Override
	public void writeUTF(@NotNull String s) throws IOException {
		out.writeUTF(s);
	}
}
