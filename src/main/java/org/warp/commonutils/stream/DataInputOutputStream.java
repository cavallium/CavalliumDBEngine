package org.warp.commonutils.stream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class DataInputOutputStream extends DataOutputStream implements DataInputOutput {

	private final DataInputStream in;

	public DataInputOutputStream(DataInputStream in, DataOutputStream out) {
		super(out);
		this.in = in;
	}

	@Override
	public DataInputStream getIn() {
		return in;
	}

	@Override
	public DataOutputStream getOut() {
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

	@Deprecated
	@Override
	public String readLine() {
		return in.readLine();
	}

	@NotNull
	@Override
	public String readUTF() {
		return in.readUTF();
	}
}
