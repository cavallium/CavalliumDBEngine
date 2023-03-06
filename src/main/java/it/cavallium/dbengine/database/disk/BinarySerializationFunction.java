package it.cavallium.dbengine.database.disk;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import org.jetbrains.annotations.Nullable;

public interface BinarySerializationFunction extends SerializationFunction<@Nullable Buf, @Nullable Buf> {}
