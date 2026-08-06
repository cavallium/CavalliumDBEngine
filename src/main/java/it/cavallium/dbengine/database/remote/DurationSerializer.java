package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataCodec;
import it.cavallium.datagen.ProjectionReadSupport;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.jetbrains.annotations.NotNull;

public class DurationSerializer implements DataCodec<Duration> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull Duration duration) {
		var units = duration.getUnits();
		var smallestUnit = (ChronoUnit) units.get(units.size() - 1);
		dataOutput.writeInt(smallestUnit.ordinal());
		dataOutput.writeLong(duration.get(smallestUnit));
	}

	@Override
	public @NotNull Duration read(SafeDataInput dataInput) {
		var smallestUnit = ChronoUnit.values()[dataInput.readInt()];
		return Duration.of(dataInput.readLong(), smallestUnit);
	}

	@Override
	public void skip(SafeDataInput dataInput) {
		ProjectionReadSupport.skipBytes(dataInput, Integer.BYTES + Long.BYTES);
	}
}
