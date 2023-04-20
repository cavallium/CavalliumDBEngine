package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataSerializer;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public class LLSnapshotSerializer implements DataSerializer<LLSnapshot> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull LLSnapshot llSnapshot) {
		dataOutput.writeLong(llSnapshot.getSequenceNumber());
	}

	@Override
	public @NotNull LLSnapshot deserialize(SafeDataInput dataInput) {
		return new LLSnapshot(dataInput.readLong());
	}
}
