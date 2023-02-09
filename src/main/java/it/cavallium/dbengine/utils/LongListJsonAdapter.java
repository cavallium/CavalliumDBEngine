package it.cavallium.dbengine.utils;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LongListJsonAdapter extends JsonAdapter<LongList> {

	@Override
	public @NotNull LongList fromJson(@NotNull JsonReader reader) {
		reader.beginArray();
		LongArrayList modifiableOutput = new LongArrayList();
		while (reader.hasNext()) {
			modifiableOutput.add(reader.nextLong());
		}
		reader.endArray();
		return LongLists.unmodifiable(modifiableOutput);
	}

	@Override
	public void toJson(@NotNull JsonWriter writer, @Nullable LongList value) {
		if (value == null) {
			writer.nullValue();
			return;
		}

		writer.beginArray();
		for (int i = 0; i < value.size(); i++) {
			writer.value(value.getLong(i));
		}
		writer.endArray();
	}
}
