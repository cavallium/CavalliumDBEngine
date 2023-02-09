package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import it.cavallium.dbengine.database.LLSnapshot;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class LLSnapshotSerializer implements DataSerializer<LLSnapshot> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull LLSnapshot llSnapshot) {
		dataOutput.writeLong(llSnapshot.getSequenceNumber());
	}

	@Override
	public @NotNull LLSnapshot deserialize(DataInput dataInput) {
		return new LLSnapshot(dataInput.readLong());
	}
}
