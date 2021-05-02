package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Map;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class SubStageGetterHashMap<T, U, TH> implements
		SubStageGetter<Map<T, U>, DatabaseMapDictionaryHashed<T, U, TH>> {

	private static final boolean assertsEnabled;
	static {
		boolean assertsEnabledTmp = false;
		//noinspection AssertWithSideEffects
		assert assertsEnabledTmp = true;
		//noinspection ConstantConditions
		assertsEnabled = assertsEnabledTmp;
	}

	private final Serializer<T, ByteBuf> keySerializer;
	private final Serializer<U, ByteBuf> valueSerializer;
	private final Function<T, TH> keyHashFunction;
	private final SerializerFixedBinaryLength<TH, ByteBuf> keyHashSerializer;

	public SubStageGetterHashMap(Serializer<T, ByteBuf> keySerializer,
			Serializer<U, ByteBuf> valueSerializer,
			Function<T, TH> keyHashFunction,
			SerializerFixedBinaryLength<TH, ByteBuf> keyHashSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.keyHashFunction = keyHashFunction;
		this.keyHashSerializer = keyHashSerializer;
	}

	@Override
	public Mono<DatabaseMapDictionaryHashed<T, U, TH>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			ByteBuf prefixKey,
			Flux<ByteBuf> debuggingKeyFlux) {
		Mono<DatabaseMapDictionaryHashed<T, U, TH>> result = Mono.fromSupplier(() -> DatabaseMapDictionaryHashed.tail(dictionary,
				prefixKey.retain(),
				keySerializer,
				valueSerializer,
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
