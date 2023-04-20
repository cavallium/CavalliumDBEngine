package it.cavallium.dbengine.database.remote;

import it.cavallium.datagen.DataSerializer;
import it.cavallium.stream.SafeDataInput;
import it.cavallium.stream.SafeDataOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.jetbrains.annotations.NotNull;

public class DurationSerializer implements DataSerializer<Duration> {

	@Override
	public void serialize(SafeDataOutput dataOutput, @NotNull Duration duration) {
		var units = duration.getUnits();
		var smallestUnit = (ChronoUnit) units.get(units.size() - 1);
		dataOutput.writeInt(smallestUnit.ordinal());
		dataOutput.writeLong(duration.get(smallestUnit));
	}

	@Override
	public @NotNull Duration deserialize(SafeDataInput dataInput) {
		var smallestUnit = ChronoUnit.values()[dataInput.readInt()];
		return Duration.of(dataInput.readLong(), smallestUnit);
	}
}
