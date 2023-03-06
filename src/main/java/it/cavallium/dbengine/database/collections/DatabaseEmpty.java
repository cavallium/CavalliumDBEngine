package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.BufDataInput;
import it.cavallium.buffer.BufDataOutput;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.Serializer;
import org.jetbrains.annotations.NotNull;

public class DatabaseEmpty {

	@SuppressWarnings({"unused", "InstantiationOfUtilityClass"})
	public static final Nothing NOTHING = new Nothing();

	public static Serializer<Nothing> nothingSerializer() {
		return new Serializer<>() {

			@Override
			public @NotNull Nothing deserialize(@NotNull BufDataInput in) throws SerializationException {
				return NOTHING;
			}

			@Override
			public void serialize(@NotNull Nothing deserialized, BufDataOutput out) throws SerializationException {

			}

			@Override
			public int getSerializedSizeHint() {
				return 0;
			}
		};
	}

	private DatabaseEmpty() {
	}

	public static DatabaseStageEntry<Nothing> create(LLDictionary dictionary, Buf key) {
		return new DatabaseMapSingle<>(dictionary, key, nothingSerializer());
	}

	public static final class Nothing {

		@SuppressWarnings("InstantiationOfUtilityClass")
		public static Nothing INSTANCE = new Nothing();

		private Nothing() {

		}
	}
}