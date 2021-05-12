package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.List;
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
			List<ByteBuf> debuggingKeys) {
		try {
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
							.fromSupplier(() -> DatabaseMapDictionaryHashed
									.tail(dictionary,
											prefixKey.retain(),
											keySerializer,
											valueSerializer,
											keyHashFunction,
											keyHashSerializer
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
		return assertsEnabled;
	}

	private Mono<Void> checkKeyFluxConsistency(ByteBuf prefixKey, List<ByteBuf> keys) {
		return Mono
				.fromCallable(() -> {
					try {
						for (ByteBuf key : keys) {
							assert key.readableBytes() == prefixKey.readableBytes() + getKeyHashBinaryLength();
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

	public int getKeyHashBinaryLength() {
		return keyHashSerializer.getSerializedBinaryLength();
	}
}
