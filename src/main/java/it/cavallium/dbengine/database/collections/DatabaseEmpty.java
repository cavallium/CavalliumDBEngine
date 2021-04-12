package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;

public class DatabaseEmpty {

	@SuppressWarnings({"unused", "InstantiationOfUtilityClass"})
	public static final Nothing NOTHING = new Nothing();
	private static final byte[] NOTHING_BYTES = new byte[0];
	public static final Serializer<Nothing, byte[]> NOTHING_SERIALIZER = new Serializer<>() {
		@Override
		public @NotNull Nothing deserialize(byte @NotNull [] serialized) {
			return NOTHING;
		}

		@Override
		public byte @NotNull [] serialize(@NotNull Nothing deserialized) {
			return NOTHING_BYTES;
		}
	};
	public static final Function<Nothing, Nothing> NOTHING_HASH_FUNCTION = nothing -> nothing;
	private static final SubStageGetter<Nothing, DatabaseStageEntry<Nothing>> NOTHING_SUB_STAGE_GETTER
			= new SubStageGetterSingle<>(NOTHING_SERIALIZER);

	private DatabaseEmpty() {
	}

	public static DatabaseStageEntry<Nothing> create(LLDictionary dictionary, byte[] key) {
		return new DatabaseSingle<>(dictionary, key, NOTHING_SERIALIZER);
	}

	public static SubStageGetter<Nothing, DatabaseStageEntry<Nothing>> createSubStageGetter() {
		return NOTHING_SUB_STAGE_GETTER;
	}

	public static final class Nothing {

		@SuppressWarnings("InstantiationOfUtilityClass")
		public static Nothing INSTANCE = new Nothing();

		private Nothing() {

		}
	}
}