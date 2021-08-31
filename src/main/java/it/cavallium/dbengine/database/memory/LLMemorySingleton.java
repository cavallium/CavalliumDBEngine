package it.cavallium.dbengine.database.memory;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.Unpooled;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class LLMemorySingleton implements LLSingleton {

	private final LLMemoryDictionary dict;
	private final byte[] singletonName;
	private final Mono<Buffer> singletonNameBufMono;

	public LLMemorySingleton(LLMemoryDictionary dict, byte[] singletonName) {
		this.dict = dict;
		this.singletonName = singletonName;
		this.singletonNameBufMono = Mono.fromCallable(() -> dict
				.getAllocator()
				.allocate(singletonName.length)
				.writeBytes(singletonName));
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
					try (b) {
						return LLUtils.toArray(b);
					}
				});
	}

	@Override
	public Mono<Void> set(byte[] value) {
		var bbKey = singletonNameBufMono;
		var bbVal = Mono.fromCallable(() -> dict.getAllocator().allocate(value.length).writeBytes(value));
		return dict
				.put(bbKey, bbVal, LLDictionaryResultType.VOID)
				.then();
	}
}
