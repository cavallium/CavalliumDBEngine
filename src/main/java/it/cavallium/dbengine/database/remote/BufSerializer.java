package it.cavallium.dbengine.database.remote;

import it.cavallium.buffer.Buf;
import it.cavallium.datagen.DataCodec;
import it.cavallium.datagen.ProjectionReadSupport;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import org.jetbrains.annotations.NotNull;

public class BufSerializer implements DataCodec<Buf> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull Buf bytes) {
		dataOutput.writeInt(bytes.size());
		for (Byte aByte : bytes) {
			dataOutput.writeByte(aByte);
		}
	}

	@Override
	public @NotNull Buf read(SafeDataInput dataInput) {
		var size = ProjectionReadSupport.readLength(dataInput);
		ProjectionReadSupport.preparePayload(dataInput, size);
		var bal = Buf.create(size);
		for (int i = 0; i < size; i++) {
			bal.add(dataInput.readByte());
		}
		return bal;
	}

	@Override
	public void skip(SafeDataInput dataInput) {
		ProjectionReadSupport.skipPayload(dataInput, ProjectionReadSupport.readLength(dataInput));
	}
}
