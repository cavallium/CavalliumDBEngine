package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.List;
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
	private final boolean enableAssertionsWhenUsingAssertions;

	public SubStageGetterMap(SerializerFixedBinaryLength<T, ByteBuf> keySerializer,
			Serializer<U, ByteBuf> valueSerializer, boolean enableAssertionsWhenUsingAssertions) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.enableAssertionsWhenUsingAssertions = enableAssertionsWhenUsingAssertions;
	}

	@Override
	public Mono<DatabaseMapDictionary<T, U>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			ByteBuf prefixKey,
			List<ByteBuf> debuggingKeys) {
		try {
			return Mono
					.defer(() -> {
						if (assertsEnabled && enableAssertionsWhenUsingAssertions) {
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
							.fromSupplier(() -> DatabaseMapDictionary
									.tail(dictionary,
											prefixKey.retain(),
											keySerializer,
											valueSerializer
									)
							)
					)
					.doFirst(prefixKey::retain)
					.doAfterTerminate(prefixKey::release);
		} finally {
			prefixKey.release();
		}
	}

	@Override
	public boolean isMultiKey() {
		return true;
	}

	@Override
	public boolean needsDebuggingKeyFlux() {
		return assertsEnabled && enableAssertionsWhenUsingAssertions;
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
		return keySerializer.getSerializedBinaryLength();
	}
}
