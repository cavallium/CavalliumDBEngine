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

	public LLMemorySingleton(LLMemoryDictionary dict, byte[] singletonName) {
		this.dict = dict;
		this.singletonName = singletonName;
	}

	@Override
	public String getDatabaseName() {
		return dict.getDatabaseName();
	}

	@Override
	public Mono<byte[]> get(@Nullable LLSnapshot snapshot) {
		var bb = Unpooled.wrappedBuffer(singletonName);
		return Mono
				.defer(() -> dict.get(snapshot, bb.retain(), false))
				.map(b -> {
					try {
						return LLUtils.toArray(b);
					} finally {
						b.release();
					}
				})
				.doAfterTerminate(bb::release)
				.doFirst(bb::retain);
	}

	@Override
	public Mono<Void> set(byte[] value) {
		var bbKey = Unpooled.wrappedBuffer(singletonName);
		var bbVal = Unpooled.wrappedBuffer(value);
		return Mono
				.defer(() -> dict
						.put(bbKey.retain(), bbVal.retain(), LLDictionaryResultType.VOID)
				)
				.doAfterTerminate(bbKey::release)
				.doAfterTerminate(bbVal::release)
				.doFirst(bbKey::retain)
				.doFirst(bbVal::retain)
				.then();
	}
}
