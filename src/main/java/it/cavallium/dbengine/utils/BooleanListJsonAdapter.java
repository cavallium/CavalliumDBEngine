package it.cavallium.dbengine.utils;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.booleans.BooleanLists;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BooleanListJsonAdapter extends JsonAdapter<BooleanList> {

	@Override
	public @NotNull BooleanList fromJson(@NotNull JsonReader reader) throws IOException {
		reader.beginArray();
		BooleanArrayList modifiableOutput = new BooleanArrayList();
		while (reader.hasNext()) {
			modifiableOutput.add(reader.nextBoolean());
		}
		reader.endArray();
		return BooleanLists.unmodifiable(modifiableOutput);
	}

	@Override
	public void toJson(@NotNull JsonWriter writer, @Nullable BooleanList value) throws IOException {
		if (value == null) {
			writer.nullValue();
			return;
		}

		writer.beginArray();
		for (int i = 0; i < value.size(); i++) {
			writer.value(value.getBoolean(i));
		}
		writer.endArray();
	}
}
