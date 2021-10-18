package it.cavallium.dbengine.database.serialization;

import java.io.DataInput;
import org.jetbrains.annotations.NotNull;

public interface BufferDataInput extends DataInput {

	@Override
	void readFully(byte @NotNull [] b);

	@Override
	void readFully(byte @NotNull [] b, int off, int len);

	@Override
	int skipBytes(int n);

	@Override
	boolean readBoolean();

	@Override
	byte readByte();

	@Override
	int readUnsignedByte();

	@Override
	short readShort();

	@Override
	int readUnsignedShort();

	@Override
	char readChar();

	@Override
	int readInt();

	@Override
	long readLong();

	@Override
	float readFloat();

	@Override
	double readDouble();

	@Override
	String readLine();

	@NotNull
	@Override
	String readUTF();

	int getReadBytesCount();
}
