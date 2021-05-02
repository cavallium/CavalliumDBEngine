package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterMap<T, U> implements SubStageGetter<Map<T, U>, DatabaseMapDictionary<T, U>> {

	private static final boolean assertsEnabled;
	static {
		boolean assertsEnabledTmp = false;
		//noinspection AssertWithSideEffects
		assert assertsEnabledTmp = true;
		//noinspection ConstantConditions
		assertsEnabled = assertsEnabledTmp;
	}

	private final SerializerFixedBinaryLength<T, ByteBuf> keySerializer;
	private final Serializer<U, ByteBuf> valueSerializer;

	public SubStageGetterMap(SerializerFixedBinaryLength<T, ByteBuf> keySerializer,
			Serializer<U, ByteBuf> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public Mono<DatabaseMapDictionary<T, U>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			ByteBuf prefixKey,
			Flux<ByteBuf> debuggingKeyFlux) {
		return Mono
				.using(
						() -> true,
						b -> Mono
								.fromSupplier(() -> DatabaseMapDictionary.tail(dictionary, prefixKey.retain(), keySerializer, valueSerializer))
								.doOnDiscard(DatabaseMapDictionary.class, DatabaseMapDictionary::release)
								.transformDeferred(result -> {
									if (assertsEnabled) {
										return this
												.checkKeyFluxConsistency(prefixKey.retain(), debuggingKeyFlux)
												.then(result);
									} else {
										return debuggingKeyFlux
												.flatMap(buf -> Mono.fromRunnable(buf::release))
												.doOnDiscard(ByteBuf.class, ReferenceCounted::release)
												.then(result);
									}
								})
								.doOnDiscard(DatabaseMapDictionary.class, DatabaseMapDictionary::release),
						b -> prefixKey.release()
				);
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
