package it.cavallium.dbengine.utils;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;
import it.unimi.dsi.fastutil.chars.CharArrayList;
import it.unimi.dsi.fastutil.chars.CharList;
import it.unimi.dsi.fastutil.chars.CharLists;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CharListJsonAdapter extends JsonAdapter<CharList> {

	@Override
	public @NotNull CharList fromJson(@NotNull JsonReader reader) throws IOException {
		reader.beginArray();
		CharArrayList modifiableOutput = new CharArrayList();
		while (reader.hasNext()) {
			modifiableOutput.add((char) reader.nextInt());
		}
		reader.endArray();
		return CharLists.unmodifiable(modifiableOutput);
	}

	@Override
	public void toJson(@NotNull JsonWriter writer, @Nullable CharList value) throws IOException {
		if (value == null) {
			writer.nullValue();
			return;
		}

		writer.beginArray();
		for (int i = 0; i < value.size(); i++) {
			writer.value((long) value.getChar(i));
		}
		writer.endArray();
	}
}
