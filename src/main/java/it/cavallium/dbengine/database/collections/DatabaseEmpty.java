package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.Serializer.DeserializationResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class DatabaseEmpty {

	@SuppressWarnings({"unused", "InstantiationOfUtilityClass"})
	public static final Nothing NOTHING = new Nothing();
	public static final DeserializationResult<Nothing> NOTHING_RESULT = new DeserializationResult<>(NOTHING, 0);

	public static Serializer<Nothing> nothingSerializer(BufferAllocator bufferAllocator) {
		return new Serializer<>() {
			@Override
			public @NotNull DeserializationResult<Nothing> deserialize(@Nullable Send<Buffer> serialized) {
				try (serialized) {
					return NOTHING_RESULT;
				}
			}

			@Override
			public @NotNull Send<Buffer> serialize(@NotNull Nothing deserialized) {
				return LLUtils.empty(bufferAllocator);
			}
		};
	}

	private DatabaseEmpty() {
	}

	public static DatabaseStageEntry<Nothing> create(LLDictionary dictionary, Send<Buffer> key, Runnable onClose) {
		return new DatabaseSingle<>(dictionary, key, nothingSerializer(dictionary.getAllocator()), onClose);
	}

	public static final class Nothing {

		@SuppressWarnings("InstantiationOfUtilityClass")
		public static Nothing INSTANCE = new Nothing();

		private Nothing() {

		}
	}
}