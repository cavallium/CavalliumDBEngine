package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import org.jetbrains.annotations.Nullable;

public interface BinarySerializationFunction extends SerializationFunction<@Nullable Buf, @Nullable Buf> {}
