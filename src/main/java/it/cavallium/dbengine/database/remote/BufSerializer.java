package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.cavallium.dbengine.buffers.Buf;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class BufSerializer implements DataSerializer<Buf> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull Buf bytes) {
		dataOutput.writeInt(bytes.size());
		for (Byte aByte : bytes) {
			dataOutput.writeByte(aByte);
		}
	}

	@Override
	public @NotNull Buf deserialize(DataInput dataInput) {
		var size = dataInput.readInt();
		var bal = Buf.create(size);
		for (int i = 0; i < size; i++) {
			bal.add(dataInput.readByte());
		}
		return bal;
	}
}
