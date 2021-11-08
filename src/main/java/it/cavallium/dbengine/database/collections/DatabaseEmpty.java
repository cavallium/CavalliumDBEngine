package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class DatabaseEmpty {

	@SuppressWarnings({"unused", "InstantiationOfUtilityClass"})
	public static final Nothing NOTHING = new Nothing();

	public static Serializer<Nothing> nothingSerializer(BufferAllocator bufferAllocator) {
		return new Serializer<>() {

			@Override
			public @NotNull Nothing deserialize(@NotNull Buffer serialized) {
				return NOTHING;
			}

			@Override
			public void serialize(@NotNull Nothing deserialized, Buffer output) {

			}

			@Override
			public int getSerializedSizeHint() {
				return 0;
			}
		};
	}

	private DatabaseEmpty() {
	}

	public static DatabaseStageEntry<Nothing> create(LLDictionary dictionary, Buffer key, Runnable onClose) {
		return new DatabaseSingle<>(dictionary, key, nothingSerializer(dictionary.getAllocator()), onClose);
	}

	public static final class Nothing {

		@SuppressWarnings("InstantiationOfUtilityClass")
		public static Nothing INSTANCE = new Nothing();

		private Nothing() {

		}
	}
}