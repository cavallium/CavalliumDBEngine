package it.cavallium.dbengine.database;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;
import org.warp.commonutils.functional.TriConsumer;
import org.warp.commonutils.functional.TriFunction;
import org.warp.commonutils.type.Bytes;
import org.warp.commonutils.type.UnmodifiableIterableMap;
import org.warp.commonutils.type.UnmodifiableMap;

@NotAtomic
public interface LLDeepDictionary extends LLKeyValueDatabaseStructure {

	UnmodifiableIterableMap<byte[], byte[]> get(@Nullable LLSnapshot snapshot, byte[] key1) throws IOException;

	Optional<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key1, byte[] key2) throws IOException;


	boolean isEmpty(@Nullable LLSnapshot snapshot, byte[] key1);

	boolean contains(@Nullable LLSnapshot snapshot, byte[] key1, byte[] key2) throws IOException;

	/**
	 * Note: this will remove previous elements because it replaces the entire map of key
	 */
	void put(byte[] key1, UnmodifiableIterableMap<byte[], byte[]> value) throws IOException;

	Optional<byte[]> put(byte[] key1, byte[] key2, byte[] value, LLDictionaryResultType resultType) throws IOException;


	void putMulti(byte[][] keys1, UnmodifiableIterableMap<byte[], byte[]>[] values) throws IOException;

	void putMulti(byte[] key1, byte[][] keys2, byte[][] values, LLDictionaryResultType resultType, Consumer<byte[]> responses) throws IOException;

	void putMulti(byte[][] keys1, byte[][] keys2, byte[][] values, LLDictionaryResultType resultType, Consumer<byte[]> responses) throws IOException;


	void clear() throws IOException;

	Optional<UnmodifiableIterableMap<byte[], byte[]>> clear(byte[] key1, LLDictionaryResultType resultType) throws IOException;

	Optional<byte[]> remove(byte[] key1, byte[] key2, LLDictionaryResultType resultType) throws IOException;


	void forEach(@Nullable LLSnapshot snapshot, int parallelism, TriConsumer<byte[], byte[], byte[]> consumer);

	void forEach(@Nullable LLSnapshot snapshot, int parallelism, BiConsumer<byte[], UnmodifiableIterableMap<byte[], byte[]>> consumer);

	void forEach(@Nullable LLSnapshot snapshot, int parallelism, byte[] key1, BiConsumer<byte[], byte[]> consumer);


	void replaceAll(int parallelism, boolean replaceKeys, TriFunction<byte[], byte[], byte[], ImmutableTriple<byte[], byte[], byte[]>> consumer) throws IOException;

	void replaceAll(int parallelism, boolean replaceKeys, BiFunction<byte[], UnmodifiableIterableMap<byte[], byte[]>, Entry<byte[], UnmodifiableMap<Bytes, byte[]>>> consumer) throws IOException;

	void replaceAll(int parallelism, boolean replaceKeys, byte[] key1, BiFunction<byte[], byte[], Entry<byte[], byte[]>> consumer) throws IOException;


	long size(@Nullable LLSnapshot snapshot, boolean fast) throws IOException;

	long exactSize(@Nullable LLSnapshot snapshot, byte[] key1);
}
