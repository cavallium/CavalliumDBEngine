package it.cavallium.dbengine.database.memory;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.util.Send;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.disk.BinarySerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class LLMemorySingleton implements LLSingleton {

	private final LLMemoryDictionary dict;
	private final String columnNameString;
	private final byte[] singletonName;
	private final Mono<Buffer> singletonNameBufMono;

	public LLMemorySingleton(LLMemoryDictionary dict, String columnNameString, byte[] singletonName) {
		this.dict = dict;
		this.columnNameString = columnNameString;
		this.singletonName = singletonName;
		this.singletonNameBufMono = Mono.fromSupplier(() -> dict.getAllocator().allocate(singletonName.length)
				.writeBytes(singletonName));
	}

	@Override
	public String getDatabaseName() {
		return dict.getDatabaseName();
	}

	@Override
	public BufferAllocator getAllocator() {
		return dict.getAllocator();
	}

	@Override
	public Mono<Buffer> get(@Nullable LLSnapshot snapshot) {
		return dict.get(snapshot, singletonNameBufMono);
	}

	@Override
	public Mono<Void> set(Mono<Buffer> value) {
		var bbKey = singletonNameBufMono;
		return dict
				.put(bbKey, value, LLDictionaryResultType.VOID)
				.then();
	}

	@Override
	public Mono<Buffer> update(BinarySerializationFunction updater,
			UpdateReturnMode updateReturnMode) {
		return dict.update(singletonNameBufMono, updater, updateReturnMode);
	}

	@Override
	public Mono<LLDelta> updateAndGetDelta(BinarySerializationFunction updater) {
		return dict.updateAndGetDelta(singletonNameBufMono, updater);
	}

	@Override
	public String getColumnName() {
		return columnNameString;
	}

	@Override
	public String getName() {
		return new String(singletonName);
	}
}
