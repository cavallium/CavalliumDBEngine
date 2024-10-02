package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.disk.RocksDBFile;
import java.util.function.Function;

public record UnsafeRawSSTEntry<T, U>(RocksDBFile file,
																	 Buf rawKey, Buf rawValue,
																	 Function<Buf, T> keyDeserializer,
																	 Function<Buf, U> valueDeserializer) {}
