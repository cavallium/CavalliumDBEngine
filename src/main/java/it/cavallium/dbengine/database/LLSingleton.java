package it.cavallium.dbengine.database;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.BinarySerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public interface LLSingleton extends LLKeyValueDatabaseStructure {


	BufferAllocator getAllocator();

	Mono<Send<Buffer>> get(@Nullable LLSnapshot snapshot);

	Mono<Void> set(Mono<Send<Buffer>> value);

	default Mono<Send<Buffer>> update(BinarySerializationFunction updater,
			UpdateReturnMode updateReturnMode) {
		return this
				.updateAndGetDelta(updater)
				.transform(prev -> LLUtils.resolveLLDelta(prev, updateReturnMode));
	}

	Mono<Send<LLDelta>> updateAndGetDelta(BinarySerializationFunction updater);

	String getColumnName();

	String getName();
}
