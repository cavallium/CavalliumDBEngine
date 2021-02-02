package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterMap<T, U> implements SubStageGetter<Map<T, U>, DatabaseStageEntry<Map<T, U>>> {

	private static final boolean assertsEnabled;
	static {
		boolean assertsEnabledTmp = false;
		//noinspection AssertWithSideEffects
		assert assertsEnabledTmp = true;
		//noinspection ConstantConditions
		assertsEnabled = assertsEnabledTmp;
	}

	private final SerializerFixedBinaryLength<T, byte[]> keySerializer;
	private final Serializer<U, byte[]> valueSerializer;

	public SubStageGetterMap(SerializerFixedBinaryLength<T, byte[]> keySerializer,
			Serializer<U, byte[]> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public Mono<DatabaseStageEntry<Map<T, U>>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] prefixKey,
			Flux<byte[]> keyFlux) {
		Mono<DatabaseStageEntry<Map<T, U>>> result = Mono.just(DatabaseMapDictionary.tail(dictionary,
				keySerializer,
				valueSerializer,
				prefixKey
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
		return keySerializer.getSerializedBinaryLength();
	}
}
