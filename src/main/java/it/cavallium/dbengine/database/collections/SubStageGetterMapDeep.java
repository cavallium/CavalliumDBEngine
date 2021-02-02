package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterMapDeep<T, U, US extends DatabaseStage<U>> implements
		SubStageGetter<Map<T, U>, DatabaseStageEntry<Map<T, U>>> {

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
	public Mono<DatabaseStageEntry<Map<T, U>>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] prefixKey,
			Flux<byte[]> keyFlux) {
		Mono<DatabaseStageEntry<Map<T, U>>> result = Mono.just(DatabaseMapDictionaryDeep.deepIntermediate(dictionary,
				subStageGetter,
				keySerializer,
				prefixKey,
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
		return assertsEnabled;
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
