package it.cavallium.dbengine.utils;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.fastutil.shorts.ShortList;
import it.unimi.dsi.fastutil.shorts.ShortLists;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ShortListJsonAdapter extends JsonAdapter<ShortList> {

	@Override
	public @NotNull ShortList fromJson(@NotNull JsonReader reader) throws IOException {
		reader.beginArray();
		ShortArrayList modifiableOutput = new ShortArrayList();
		while (reader.hasNext()) {
			modifiableOutput.add((short) reader.nextInt());
		}
		reader.endArray();
		return ShortLists.unmodifiable(modifiableOutput);
	}

	@Override
	public void toJson(@NotNull JsonWriter writer, @Nullable ShortList value) throws IOException {
		if (value == null) {
			writer.nullValue();
			return;
		}

		writer.beginArray();
		for (int i = 0; i < value.size(); i++) {
			writer.value((long) value.getShort(i));
		}
		writer.endArray();
	}
}
