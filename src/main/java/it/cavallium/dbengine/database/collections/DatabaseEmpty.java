package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.Serializer.DeserializationResult;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;

public class DatabaseEmpty {

	@SuppressWarnings({"unused", "InstantiationOfUtilityClass"})
	public static final Nothing NOTHING = new Nothing();
	public static final DeserializationResult<Nothing> NOTHING_RESULT = new DeserializationResult<>(NOTHING, 0);

	public static Serializer<Nothing> nothingSerializer(BufferAllocator bufferAllocator) {
		return new Serializer<>() {
			@Override
			public @NotNull DeserializationResult<Nothing> deserialize(@NotNull Send<Buffer> serialized) {
				try (serialized) {
					return NOTHING_RESULT;
				}
			}

			@Override
			public @NotNull Send<Buffer> serialize(@NotNull Nothing deserialized) {
				return bufferAllocator.allocate(0).send();
			}
		};
	}

	private DatabaseEmpty() {
	}

	public static DatabaseStageEntry<Nothing> create(LLDictionary dictionary, Send<Buffer> key) {
		return new DatabaseSingle<>(dictionary, key, nothingSerializer(dictionary.getAllocator()));
	}

	public static final class Nothing {

		@SuppressWarnings("InstantiationOfUtilityClass")
		public static Nothing INSTANCE = new Nothing();

		private Nothing() {

		}
	}
}