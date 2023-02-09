package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.cavallium.dbengine.database.UpdateReturnMode;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class UpdateReturnModeSerializer implements DataSerializer<UpdateReturnMode> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull UpdateReturnMode updateReturnMode) {
		dataOutput.writeInt(updateReturnMode.ordinal());
	}

	@Override
	public @NotNull UpdateReturnMode deserialize(DataInput dataInput) {
		return UpdateReturnMode.values()[dataInput.readInt()];
	}
}
