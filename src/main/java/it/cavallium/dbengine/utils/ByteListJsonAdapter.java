package it.cavallium.dbengine.utils;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.bytes.ByteLists;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ByteListJsonAdapter extends JsonAdapter<ByteList> {

	@Override
	public @NotNull ByteList fromJson(@NotNull JsonReader reader) throws IOException {
		reader.beginArray();
		ByteArrayList modifiableOutput = new ByteArrayList();
		while (reader.hasNext()) {
			modifiableOutput.add((byte) reader.nextInt());
		}
		reader.endArray();
		return ByteLists.unmodifiable(modifiableOutput);
	}

	@Override
	public void toJson(@NotNull JsonWriter writer, @Nullable ByteList value) throws IOException {
		if (value == null) {
			writer.nullValue();
			return;
		}

		writer.beginArray();
		for (int i = 0; i < value.size(); i++) {
			writer.value((long) value.getByte(i));
		}
		writer.endArray();
	}
}
