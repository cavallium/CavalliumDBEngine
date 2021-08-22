package it.cavallium.dbengine.database.memory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class LLMemorySingleton implements LLSingleton {

	private final LLMemoryDictionary dict;
	private final byte[] singletonName;
	private final Mono<ByteBuf> singletonNameBufMono;

	public LLMemorySingleton(LLMemoryDictionary dict, byte[] singletonName) {
		this.dict = dict;
		this.singletonName = singletonName;
		ByteBuf singletonNameBuf = Unpooled.wrappedBuffer(singletonName);
		this.singletonNameBufMono = Mono.just(singletonNameBuf).map(ByteBuf::retain);
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
					try {
						return LLUtils.toArray(b);
					} finally {
						b.release();
					}
				});
	}

	@Override
	public Mono<Void> set(byte[] value) {
		var bbKey = Mono.just(Unpooled.wrappedBuffer(singletonName)).map(ByteBuf::retain);
		var bbVal = Mono.just(Unpooled.wrappedBuffer(value)).map(ByteBuf::retain);
		return dict
				.put(bbKey, bbVal, LLDictionaryResultType.VOID)
				.then();
	}
}
