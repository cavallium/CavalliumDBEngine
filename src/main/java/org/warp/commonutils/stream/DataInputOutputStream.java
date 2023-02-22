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

	@Deprecated
	@Override
	public String readLine() throws IOException {
		return in.readLine();
	}

	@NotNull
	@Override
	public String readUTF() throws IOException {
		return in.readUTF();
	}
}
