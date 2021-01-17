package it.cavallium.dbengine.database;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;
import org.warp.commonutils.functional.CancellableBiConsumer;
import org.warp.commonutils.functional.CancellableBiFunction;
import org.warp.commonutils.functional.ConsumerResult;

@NotAtomic
public interface LLDictionary extends LLKeyValueDatabaseStructure {

	Optional<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key) throws IOException;

	boolean contains(@Nullable LLSnapshot snapshot, byte[] key) throws IOException;

	Optional<byte[]> put(byte[] key, byte[] value, LLDictionaryResultType resultType)
			throws IOException;

	void putMulti(byte[][] key, byte[][] value, LLDictionaryResultType resultType,
			Consumer<byte[]> responses) throws IOException;

	Optional<byte[]> remove(byte[] key, LLDictionaryResultType resultType) throws IOException;

	/**
	 * This method can call the consumer from different threads in parallel
	 */
	ConsumerResult forEach(@Nullable LLSnapshot snapshot, int parallelism, CancellableBiConsumer<byte[], byte[]> consumer);

	/**
	 * This method can call the consumer from different threads in parallel
	 */
	ConsumerResult replaceAll(int parallelism, boolean replaceKeys, CancellableBiFunction<byte[], byte[], Entry<byte[], byte[]>> consumer) throws IOException;

	void clear() throws IOException;

	long size(@Nullable LLSnapshot snapshot, boolean fast) throws IOException;

	boolean isEmpty(@Nullable LLSnapshot snapshot) throws IOException;

	Optional<Entry<byte[], byte[]>> removeOne() throws IOException;
}
