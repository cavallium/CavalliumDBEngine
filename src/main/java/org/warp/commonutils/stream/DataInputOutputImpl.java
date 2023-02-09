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
	public void readFully(byte @NotNull [] bytes) {
		in.readFully(bytes);
	}

	@Override
	public void readFully(byte @NotNull [] bytes, int i, int i1) {
		in.readFully(bytes, i, i1);
	}

	@Override
	public int skipBytes(int i) {
		return in.skipBytes(i);
	}

	@Override
	public boolean readBoolean() {
		return in.readBoolean();
	}

	@Override
	public byte readByte() {
		return in.readByte();
	}

	@Override
	public int readUnsignedByte() {
		return in.readUnsignedByte();
	}

	@Override
	public short readShort() {
		return in.readShort();
	}

	@Override
	public int readUnsignedShort() {
		return in.readUnsignedShort();
	}

	@Override
	public char readChar() {
		return in.readChar();
	}

	@Override
	public int readInt() {
		return in.readInt();
	}

	@Override
	public long readLong() {
		return in.readLong();
	}

	@Override
	public float readFloat() {
		return in.readFloat();
	}

	@Override
	public double readDouble() {
		return in.readDouble();
	}

	@Override
	public String readLine() {
		return in.readLine();
	}

	@NotNull
	@Override
	public String readUTF() {
		return in.readUTF();
	}

	@Override
	public void write(int i) {
		out.write(i);
	}

	@Override
	public void write(byte @NotNull [] bytes) {
		out.write(bytes);
	}

	@Override
	public void write(byte @NotNull [] bytes, int i, int i1) {
		out.write(bytes, i, i1);
	}

	@Override
	public void writeBoolean(boolean b) {
		out.writeBoolean(b);
	}

	@Override
	public void writeByte(int i) {
		out.writeByte(i);
	}

	@Override
	public void writeShort(int i) {
		out.writeShort(i);
	}

	@Override
	public void writeChar(int i) {
		out.writeChar(i);
	}

	@Override
	public void writeInt(int i) {
		out.writeInt(i);
	}

	@Override
	public void writeLong(long l) {
		out.writeLong(l);
	}

	@Override
	public void writeFloat(float v) {
		out.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) {
		out.writeDouble(v);
	}

	@Override
	public void writeBytes(@NotNull String s) {
		out.writeBytes(s);
	}

	@Override
	public void writeChars(@NotNull String s) {
		out.writeChars(s);
	}

	@Override
	public void writeUTF(@NotNull String s) {
		out.writeUTF(s);
	}
}
