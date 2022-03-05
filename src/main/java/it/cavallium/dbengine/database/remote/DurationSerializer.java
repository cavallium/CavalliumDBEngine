package it.cavallium.dbengine.database.remote;

import it.cavallium.data.generator.DataSerializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import org.jetbrains.annotations.NotNull;

public class DurationSerializer implements DataSerializer<Duration> {

	@Override
	public void serialize(DataOutput dataOutput, @NotNull Duration duration) throws IOException {
		var units = duration.getUnits();
		var smallestUnit = (ChronoUnit) units.get(units.size() - 1);
		dataOutput.writeInt(smallestUnit.ordinal());
		dataOutput.writeLong(duration.get(smallestUnit));
	}

	@Override
	public @NotNull Duration deserialize(DataInput dataInput) throws IOException {
		var smallestUnit = ChronoUnit.values()[dataInput.readInt()];
		return Duration.of(dataInput.readLong(), smallestUnit);
	}
}