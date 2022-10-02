package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.Buffer;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import org.jetbrains.annotations.Nullable;

public interface BinarySerializationFunction extends SerializationFunction<@Nullable Buffer, @Nullable Buffer> {}
