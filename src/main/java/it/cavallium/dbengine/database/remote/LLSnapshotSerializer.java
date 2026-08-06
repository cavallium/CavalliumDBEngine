package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataCodec;
import it.cavallium.datagen.ProjectionReadSupport;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import org.jetbrains.annotations.NotNull;

public class LLSnapshotSerializer implements DataCodec<LLSnapshot> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull LLSnapshot llSnapshot) {
		dataOutput.writeLong(llSnapshot.getSequenceNumber());
	}

	@Override
	public @NotNull LLSnapshot read(SafeDataInput dataInput) {
		return new LLSnapshot(dataInput.readLong());
	}

	@Override
	public void skip(SafeDataInput dataInput) {
		ProjectionReadSupport.skipBytes(dataInput, Long.BYTES);
	}
}
