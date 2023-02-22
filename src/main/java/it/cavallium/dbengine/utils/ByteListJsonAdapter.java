package it.cavallium.dbengine.utils;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;
import it.cavallium.dbengine.buffers.Buf;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ByteListJsonAdapter extends JsonAdapter<Buf> {

	@Override
	public @NotNull Buf fromJson(@NotNull JsonReader reader) throws IOException {
		reader.beginArray();
		var modifiableOutput = Buf.create();
		while (reader.hasNext()) {
			modifiableOutput.add((byte) reader.nextInt());
		}
		reader.endArray();
		return modifiableOutput;
	}

	@Override
	public void toJson(@NotNull JsonWriter writer, @Nullable Buf value) throws IOException {
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
