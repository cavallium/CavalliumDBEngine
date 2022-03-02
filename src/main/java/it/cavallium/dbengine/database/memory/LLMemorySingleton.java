package it.cavallium.dbengine.database.memory;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.disk.UpdateAtomicResult;
import it.cavallium.dbengine.database.disk.UpdateAtomicResultCurrent;
import it.cavallium.dbengine.database.disk.UpdateAtomicResultMode;
import it.cavallium.dbengine.database.disk.UpdateAtomicResultPrevious;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class LLMemorySingleton implements LLSingleton {

	private final LLMemoryDictionary dict;
	private final byte[] singletonName;
	private final Mono<Send<Buffer>> singletonNameBufMono;

	public LLMemorySingleton(LLMemoryDictionary dict, byte[] singletonName) {
		this.dict = dict;
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
	public Mono<byte[]> get(@Nullable LLSnapshot snapshot) {
		return dict
				.get(snapshot, singletonNameBufMono, false)
				.map(b -> {
					try (var buf = b.receive()) {
						return LLUtils.toArray(buf);
					}
				});
	}

	@Override
	public Mono<Void> set(byte[] value) {
		var bbKey = singletonNameBufMono;
		var bbVal = Mono.fromCallable(() -> dict.getAllocator().allocate(value.length).writeBytes(value).send());
		return dict
				.put(bbKey, bbVal, LLDictionaryResultType.VOID)
				.then();
	}

	@Override
	public Mono<Send<Buffer>> update(SerializationFunction<@Nullable Send<Buffer>, @Nullable Buffer> updater,
			UpdateReturnMode updateReturnMode) {
		return dict.update(singletonNameBufMono, updater, updateReturnMode);
	}
}
