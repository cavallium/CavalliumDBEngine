package it.cavallium.dbengine.database.collections;

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

	private final SerializerFixedBinaryLength<T, byte[]> keySerializer;

	public SubStageGetterSet(SerializerFixedBinaryLength<T, byte[]> keySerializer) {
		this.keySerializer = keySerializer;
	}

	@Override
	public Mono<DatabaseSetDictionary<T>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] prefixKey,
			Flux<byte[]> keyFlux) {
		Mono<DatabaseSetDictionary<T>> result = Mono.just(DatabaseSetDictionary.tail(dictionary, prefixKey, keySerializer));
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
