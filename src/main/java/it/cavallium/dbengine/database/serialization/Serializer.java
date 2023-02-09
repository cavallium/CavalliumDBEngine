package it.cavallium.dbengine.database.serialization;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.buffers.BufDataInput;
import it.cavallium.dbengine.buffers.BufDataOutput;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;

public interface Serializer<A> {

	@NotNull A deserialize(@NotNull BufDataInput in) throws SerializationException;

	void serialize(@NotNull A deserialized, BufDataOutput out) throws SerializationException;

	/**
	 * @return suggested default buffer size, -1 if unknown
	 */
	int getSerializedSizeHint();

	Serializer<Buf> NOOP_SERIALIZER = new Serializer<>() {
		@Override
		public @NotNull Buf deserialize(@NotNull BufDataInput in) {
			return Buf.wrap(in.readAllBytes());
		}

		@Override
		public void serialize(@NotNull Buf deserialized, BufDataOutput out) {
			out.writeBytes(deserialized);
		}

		@Override
		public int getSerializedSizeHint() {
			return -1;
		}
	};

		Serializer<String> UTF8_SERIALIZER = new Serializer<>() {

			@Override
			public @NotNull String deserialize(@NotNull BufDataInput in) throws SerializationException {
				int length = in.readInt();
				var bytes = in.readNBytes(length);
				return new String(bytes, StandardCharsets.UTF_8);
			}

			@Override
			public void serialize(@NotNull String deserialized, BufDataOutput out) throws SerializationException {
			var bytes = deserialized.getBytes(StandardCharsets.UTF_8);
			out.ensureWritable(Integer.BYTES + bytes.length);
			out.writeInt(bytes.length);
			out.write(bytes);
		}

		@Override
		public int getSerializedSizeHint() {
			return -1;
		}
	};
}
