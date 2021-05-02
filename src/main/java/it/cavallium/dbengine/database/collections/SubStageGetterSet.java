package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterSet<T> implements SubStageGetter<Map<T, Nothing>, DatabaseSetDictionary<T>> {

	private static final boolean assertsEnabled;
	static {
		boolean assertsEnabledTmp = false;
		//noinspection AssertWithSideEffects
		assert assertsEnabledTmp = true;
		//noinspection ConstantConditions
		assertsEnabled = assertsEnabledTmp;
	}

	private final SerializerFixedBinaryLength<T, ByteBuf> keySerializer;

	public SubStageGetterSet(SerializerFixedBinaryLength<T, ByteBuf> keySerializer) {
		this.keySerializer = keySerializer;
	}

	@Override
	public Mono<DatabaseSetDictionary<T>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			ByteBuf prefixKey,
			Flux<ByteBuf> debuggingKeyFlux) {
		Mono<DatabaseSetDictionary<T>> result = Mono
				.fromSupplier(() -> DatabaseSetDictionary.tail(dictionary, prefixKey.retain(), keySerializer));
		if (assertsEnabled) {
			return checkKeyFluxConsistency(prefixKey.retain(), debuggingKeyFlux)
					.then(result)
					.doFinally(s -> prefixKey.release());
		} else {
			return debuggingKeyFlux
					.flatMap(key -> Mono.fromRunnable(key::release))
					.then(result)
					.doFinally(s -> prefixKey.release());
		}
	}

	@Override
	public boolean isMultiKey() {
		return true;
	}

	@Override
	public boolean needsDebuggingKeyFlux() {
		return assertsEnabled;
	}

	private Mono<Void> checkKeyFluxConsistency(ByteBuf prefixKey, Flux<ByteBuf> keyFlux) {
		return keyFlux
				.doOnNext(key -> {
					assert key.readableBytes() == prefixKey.readableBytes() + getKeyBinaryLength();
				})
				.flatMap(key -> Mono.fromRunnable(key::release))
				.doOnDiscard(ByteBuf.class, ReferenceCounted::release)
				.then()
				.doFinally(s -> prefixKey.release());
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength();
	}
}
