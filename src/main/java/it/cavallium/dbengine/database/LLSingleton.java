package it.cavallium.dbengine.database;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.io.IOException;
import org.jetbrains.annotations.Nullable;

public interface LLSingleton extends LLKeyValueDatabaseStructure {

	Buf get(@Nullable LLSnapshot snapshot);

	void set(Buf value);

	default Buf update(SerializationFunction<@Nullable Buf, @Nullable Buf> updater, UpdateReturnMode updateReturnMode) {
		var prev = this.updateAndGetDelta(updater);
		return LLUtils.resolveLLDelta(prev, updateReturnMode);
	}

	LLDelta updateAndGetDelta(SerializationFunction<@Nullable Buf, @Nullable Buf> updater);

	String getColumnName();

	String getName();
}
