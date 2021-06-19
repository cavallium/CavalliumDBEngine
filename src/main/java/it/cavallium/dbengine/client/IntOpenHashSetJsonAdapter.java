package it.cavallium.dbengine.client;

import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;
import it.cavallium.data.generator.nativedata.Int52;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class IntOpenHashSetJsonAdapter extends com.squareup.moshi.JsonAdapter<IntOpenHashSet> {

	@Nullable
	@Override
	public IntOpenHashSet fromJson(@NotNull JsonReader reader) throws IOException {
		var intOpenHashSet = new IntOpenHashSet();
		while (reader.hasNext()) {
			intOpenHashSet.add(reader.nextInt());
		}
		return intOpenHashSet;
	}

	@Override
	public void toJson(@NotNull JsonWriter writer, @Nullable IntOpenHashSet value) throws IOException {
		if (value == null) {
			writer.nullValue();
		} else {
			writer.beginArray();
			for (int integer : value) {
				writer.value(integer);
			}
			writer.endArray();
		}
	}
}
