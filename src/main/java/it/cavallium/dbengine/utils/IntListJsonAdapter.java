package it.cavallium.dbengine.utils;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class IntListJsonAdapter extends JsonAdapter<IntList> {

	@Override
	public @NotNull IntList fromJson(@NotNull JsonReader reader) throws IOException {
		reader.beginArray();
		IntArrayList modifiableOutput = new IntArrayList();
		while (reader.hasNext()) {
			modifiableOutput.add(reader.nextInt());
		}
		reader.endArray();
		return IntLists.unmodifiable(modifiableOutput);
	}

	@Override
	public void toJson(@NotNull JsonWriter writer, @Nullable IntList value) throws IOException {
		if (value == null) {
			writer.nullValue();
			return;
		}

		writer.beginArray();
		for (int i = 0; i < value.size(); i++) {
			writer.value((long) value.getInt(i));
		}
		writer.endArray();
	}
}
