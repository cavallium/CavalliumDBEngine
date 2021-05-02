package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterMapDeep<T, U, US extends DatabaseStage<U>> implements SubStageGetter<Map<T, U>, DatabaseMapDictionaryDeep<T, U, US>> {

	private static final boolean assertsEnabled;
	static {
		boolean assertsEnabledTmp = false;
		//noinspection AssertWithSideEffects
		assert assertsEnabledTmp = true;
		//noinspection ConstantConditions
		assertsEnabled = assertsEnabledTmp;
	}

	private final SubStageGetter<U, US> subStageGetter;
	private final SerializerFixedBinaryLength<T, ByteBuf> keySerializer;
	private final int keyExtLength;

	public SubStageGetterMapDeep(SubStageGetter<U, US> subStageGetter,
			SerializerFixedBinaryLength<T, ByteBuf> keySerializer,
			int keyExtLength) {
		this.subStageGetter = subStageGetter;
		this.keySerializer = keySerializer;
		this.keyExtLength = keyExtLength;
		assert keyExtConsistency();
	}


	private boolean keyExtConsistency() {
		if (subStageGetter instanceof SubStageGetterMapDeep) {
			return keyExtLength == ((SubStageGetterMapDeep<?, ?, ?>) subStageGetter).getKeyBinaryLength();
		} else if (subStageGetter instanceof SubStageGetterMap) {
			return keyExtLength == ((SubStageGetterMap<?, ?>) subStageGetter).getKeyBinaryLength();
		} else {
			return true;
		}
	}

	@Override
	public Mono<DatabaseMapDictionaryDeep<T, U, US>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			ByteBuf prefixKey,
			Flux<ByteBuf> debuggingKeyFlux) {
		return Flux
				.defer(() -> {
					if (assertsEnabled) {
						return this
								.checkKeyFluxConsistency(prefixKey.retain(), debuggingKeyFlux);
					} else {
						return debuggingKeyFlux.flatMap(buf -> Mono.fromRunnable(buf::release));
					}
				})
				.then(Mono
						.fromSupplier(() -> DatabaseMapDictionaryDeep.deepIntermediate(dictionary,
								prefixKey.retain(),
								keySerializer,
								subStageGetter,
								keyExtLength
						))
				)
				.doFinally(s -> prefixKey.release());
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
		return keySerializer.getSerializedBinaryLength() + keyExtLength;
	}
}
