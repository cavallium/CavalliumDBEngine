package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class ByteListSerializer implements DataSerializer<ByteList> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull ByteList bytes) throws IOException {
		dataOutput.writeInt(bytes.size());
		for (Byte aByte : bytes) {
			dataOutput.writeByte(aByte);
		}
	}

	@Override
	public @NotNull ByteList deserialize(DataInput dataInput) throws IOException {
		var size = dataInput.readInt();
		var bal = new ByteArrayList(size);
		for (int i = 0; i < size; i++) {
			bal.add(dataInput.readByte());
		}
		return bal;
	}
}
