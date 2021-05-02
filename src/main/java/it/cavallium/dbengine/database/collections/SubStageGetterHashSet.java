package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Map;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class SubStageGetterHashSet<T, TH> implements
		SubStageGetter<Map<T, Nothing>, DatabaseSetDictionaryHashed<T, TH>> {

	private static final boolean assertsEnabled;
	static {
		boolean assertsEnabledTmp = false;
		//noinspection AssertWithSideEffects
		assert assertsEnabledTmp = true;
		//noinspection ConstantConditions
		assertsEnabled = assertsEnabledTmp;
	}

	private final Serializer<T, ByteBuf> keySerializer;
	private final Function<T, TH> keyHashFunction;
	private final SerializerFixedBinaryLength<TH, ByteBuf> keyHashSerializer;

	public SubStageGetterHashSet(Serializer<T, ByteBuf> keySerializer,
			Function<T, TH> keyHashFunction,
			SerializerFixedBinaryLength<TH, ByteBuf> keyHashSerializer) {
		this.keySerializer = keySerializer;
		this.keyHashFunction = keyHashFunction;
		this.keyHashSerializer = keyHashSerializer;
	}

	@Override
	public Mono<DatabaseSetDictionaryHashed<T, TH>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			ByteBuf prefixKey,
			Flux<ByteBuf> debuggingKeyFlux) {
		Mono<DatabaseSetDictionaryHashed<T, TH>> result = Mono.fromSupplier(() -> DatabaseSetDictionaryHashed.tail(dictionary,
				prefixKey.retain(),
				keySerializer,
				keyHashFunction,
				keyHashSerializer
		));
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
					assert key.readableBytes() == prefixKey.readableBytes() + getKeyHashBinaryLength();
				})
				.flatMap(key -> Mono.fromRunnable(key::release))
				.doOnDiscard(ByteBuf.class, ReferenceCounted::release)
				.then()
				.doFinally(s -> prefixKey.release());
	}

	public int getKeyHashBinaryLength() {
		return keyHashSerializer.getSerializedBinaryLength();
	}
}
