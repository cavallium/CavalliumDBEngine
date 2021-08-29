package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import static io.netty.buffer.Unpooled.*;

public class DatabaseEmpty {

	@SuppressWarnings({"unused", "InstantiationOfUtilityClass"})
	public static final Nothing NOTHING = new Nothing();
	public static final Serializer<Nothing, Buffer> NOTHING_SERIALIZER = new Serializer<>() {
		@Override
		public @NotNull Nothing deserialize(@NotNull Buffer serialized) {
			try {
				return NOTHING;
			} finally {
				serialized.release();
			}
		}

		@Override
		public @NotNull Buffer serialize(@NotNull Nothing deserialized) {
			return EMPTY_BUFFER;
		}
	};
	public static final Function<Nothing, Nothing> NOTHING_HASH_FUNCTION = nothing -> nothing;
	private static final SubStageGetter<Nothing, DatabaseStageEntry<Nothing>> NOTHING_SUB_STAGE_GETTER
			= new SubStageGetterSingle<>(NOTHING_SERIALIZER);

	private DatabaseEmpty() {
	}

	public static DatabaseStageEntry<Nothing> create(LLDictionary dictionary, Buffer key) {
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