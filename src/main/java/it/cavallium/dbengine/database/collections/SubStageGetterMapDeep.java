package it.cavallium.dbengine.database.collections;

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
	private final SerializerFixedBinaryLength<T, byte[]> keySerializer;
	private final int keyExtLength;

	public SubStageGetterMapDeep(SubStageGetter<U, US> subStageGetter,
			SerializerFixedBinaryLength<T, byte[]> keySerializer,
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
			byte[] prefixKey,
			Flux<byte[]> keyFlux) {
		Mono<DatabaseMapDictionaryDeep<T, U, US>> result = Mono.just(DatabaseMapDictionaryDeep.deepIntermediate(dictionary,
				prefixKey,
				keySerializer,
				subStageGetter,
				keyExtLength
		));
		if (assertsEnabled) {
			return checkKeyFluxConsistency(prefixKey, keyFlux).then(result);
		} else {
			return result;
		}
	}

	@Override
	public boolean needsKeyFlux() {
		return true;
	}

	private Mono<Void> checkKeyFluxConsistency(byte[] prefixKey, Flux<byte[]> keyFlux) {
		return keyFlux.doOnNext(key -> {
			assert key.length == prefixKey.length + getKeyBinaryLength();
		}).then();
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength() + keyExtLength;
	}
}
