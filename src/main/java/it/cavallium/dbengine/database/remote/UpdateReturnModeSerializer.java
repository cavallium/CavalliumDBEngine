package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataCodec;
import it.cavallium.datagen.ProjectionReadSupport;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import org.jetbrains.annotations.NotNull;

public class UpdateReturnModeSerializer implements DataCodec<UpdateReturnMode> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull UpdateReturnMode updateReturnMode) {
		dataOutput.writeInt(updateReturnMode.ordinal());
	}

	@Override
	public @NotNull UpdateReturnMode read(SafeDataInput dataInput) {
		return UpdateReturnMode.values()[dataInput.readInt()];
	}

	@Override
	public void skip(SafeDataInput dataInput) {
		ProjectionReadSupport.skipBytes(dataInput, Integer.BYTES);
	}
}
