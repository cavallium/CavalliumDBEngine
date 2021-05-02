package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.List;
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
			List<ByteBuf> debuggingKeys) {
		return Mono
				.defer(() -> {
					if (assertsEnabled) {
						return checkKeyFluxConsistency(prefixKey.retain(), debuggingKeys);
					} else {
						return Mono
								.fromCallable(() -> {
									for (ByteBuf key : debuggingKeys) {
										key.release();
									}
									return null;
								});
					}
				})
				.then(Mono
						.fromSupplier(() -> DatabaseMapDictionaryDeep
								.deepIntermediate(dictionary,
										prefixKey.retain(),
										keySerializer,
										subStageGetter,
										keyExtLength
								)
						)
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

	private Mono<Void> checkKeyFluxConsistency(ByteBuf prefixKey, List<ByteBuf> keys) {
		return Mono
				.fromCallable(() -> {
					try {
						for (ByteBuf key : keys) {
							assert key.readableBytes() == prefixKey.readableBytes() + getKeyBinaryLength();
						}
					} finally {
						prefixKey.release();
						for (ByteBuf key : keys) {
							key.release();
						}
					}
					return null;
				});
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength() + keyExtLength;
	}
}
