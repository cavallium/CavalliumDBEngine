package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;

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

	public static DatabaseStageEntry<Nothing> create(LLDictionary dictionary, BufSupplier key) {
		return new DatabaseMapSingle<>(dictionary, key, nothingSerializer(dictionary.getAllocator()));
	}

	public static final class Nothing {

		@SuppressWarnings("InstantiationOfUtilityClass")
		public static Nothing INSTANCE = new Nothing();

		private Nothing() {

		}
	}
}