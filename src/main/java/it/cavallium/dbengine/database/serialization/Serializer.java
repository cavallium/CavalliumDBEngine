package it.cavallium.dbengine.database.serialization;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.util.Send;
import io.netty5.util.internal.StringUtil;
import it.cavallium.dbengine.database.LLUtils;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface Serializer<A> {

	/**
	 *
	 * @param serialized the serialized data should be split!
	 */
	@NotNull A deserialize(@NotNull Buffer serialized) throws SerializationException;

	/**
	 * @param output its writable size will be at least equal to the size hint
	 */
	void serialize(@NotNull A deserialized, Buffer output) throws SerializationException;

	/**
	 * @return suggested default buffer size, -1 if unknown
	 */
	int getSerializedSizeHint();

	Serializer<Buffer> NOOP_SERIALIZER = new Serializer<>() {
		@Override
		public @NotNull Buffer deserialize(@NotNull Buffer serialized) {
			return serialized.split();
		}

		@Override
		public void serialize(@NotNull Buffer deserialized, @NotNull Buffer deserializedToReceive) {
			deserializedToReceive.ensureWritable(deserialized.readableBytes());
			deserializedToReceive.writeBytes(deserialized);
		}

		@Override
		public int getSerializedSizeHint() {
			return -1;
		}
	};

	Serializer<Send<Buffer>> NOOP_SEND_SERIALIZER = new Serializer<>() {
		@Override
		public @NotNull Send<Buffer> deserialize(@NotNull Buffer serialized) {
			return serialized.split().send();
		}

		@Override
		public void serialize(@NotNull Send<Buffer> deserialized, @NotNull Buffer deserializedToReceive) {
			try (var received = deserialized.receive()) {
				deserializedToReceive.ensureWritable(received.readableBytes());
				deserializedToReceive.writeBytes(received);
			}
		}

		@Override
		public int getSerializedSizeHint() {
			return -1;
		}
	};

	
		Serializer<String> UTF8_SERIALIZER = new Serializer<>() {
		@Override
		public @NotNull String deserialize(@NotNull Buffer serialized) {
			assert serialized.isAccessible();
			int length = serialized.readInt();
			try (var strBuf = serialized.readSplit(length)) {
				return LLUtils.deserializeString(strBuf, strBuf.readerOffset(), length, StandardCharsets.UTF_8);
			}
		}

		@Override
		public void serialize(@NotNull String deserialized, Buffer output) {
			var bytes =  deserialized.getBytes(StandardCharsets.UTF_8);
			output.ensureWritable(Integer.BYTES + bytes.length);
			output.writeInt(bytes.length);
			output.writeBytes(bytes);
		}

		@Override
		public int getSerializedSizeHint() {
			return -1;
		}
	};
}
