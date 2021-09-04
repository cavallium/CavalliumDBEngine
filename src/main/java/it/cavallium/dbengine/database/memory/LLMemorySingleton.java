package it.cavallium.dbengine.database.memory;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

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
}
