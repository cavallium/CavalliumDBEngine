package it.cavallium.dbengine.database.memory;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class LLMemorySingleton implements LLSingleton {

	private final LLMemoryDictionary dict;
	private final String columnNameString;
	private final byte[] singletonName;
	private final Mono<Send<Buffer>> singletonNameBufMono;

	public LLMemorySingleton(LLMemoryDictionary dict, String columnNameString, byte[] singletonName) {
		this.dict = dict;
		this.columnNameString = columnNameString;
		this.singletonName = singletonName;
		this.singletonNameBufMono = Mono.fromCallable(() -> dict
				.getAllocator()
				.allocate(singletonName.length)
				.writeBytes(singletonName)
				.send());
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
	public Mono<Send<Buffer>> get(@Nullable LLSnapshot snapshot) {
		return dict.get(snapshot, singletonNameBufMono);
	}

	@Override
	public Mono<Void> set(Mono<Send<Buffer>> value) {
		var bbKey = singletonNameBufMono;
		return dict
				.put(bbKey, value, LLDictionaryResultType.VOID)
				.then();
	}

	@Override
	public Mono<Send<Buffer>> update(SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
			UpdateReturnMode updateReturnMode) {
		return dict.update(singletonNameBufMono, updater, updateReturnMode);
	}

	@Override
	public Mono<Send<LLDelta>> updateAndGetDelta(SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater) {
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
