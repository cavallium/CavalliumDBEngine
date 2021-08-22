package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
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
	private final boolean enableAssertionsWhenUsingAssertions;

	public SubStageGetterHashMap(Serializer<T, ByteBuf> keySerializer,
			Serializer<U, ByteBuf> valueSerializer,
			Function<T, TH> keyHashFunction,
			SerializerFixedBinaryLength<TH, ByteBuf> keyHashSerializer,
			boolean enableAssertionsWhenUsingAssertions) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.keyHashFunction = keyHashFunction;
		this.keyHashSerializer = keyHashSerializer;
		this.enableAssertionsWhenUsingAssertions = enableAssertionsWhenUsingAssertions;
	}

	@Override
	public Mono<DatabaseMapDictionaryHashed<T, U, TH>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<ByteBuf> prefixKeyMono,
			Flux<ByteBuf> debuggingKeysFlux) {
		return Mono.usingWhen(
				prefixKeyMono,
				prefixKey -> Mono
						.fromSupplier(() -> DatabaseMapDictionaryHashed
								.tail(dictionary,
										prefixKey.retain(),
										keySerializer,
										valueSerializer,
										keyHashFunction,
										keyHashSerializer
								)
						)
						.transform(mono -> {
							if (assertsEnabled && enableAssertionsWhenUsingAssertions) {
								return debuggingKeysFlux.handle((key, sink) -> {
									try {
										if (key.readableBytes() != prefixKey.readableBytes() + getKeyHashBinaryLength()) {
											sink.error(new IndexOutOfBoundsException());
										} else {
											sink.complete();
										}
									} finally {
										key.release();
									}
								}).then(mono);
							} else {
								return mono;
							}
						}),
				prefixKey -> Mono.fromRunnable(prefixKey::release)
		);
	}

	@Override
	public boolean isMultiKey() {
		return true;
	}

	@Override
	public boolean needsDebuggingKeyFlux() {
		return assertsEnabled && enableAssertionsWhenUsingAssertions;
	}

	public int getKeyHashBinaryLength() {
		return keyHashSerializer.getSerializedBinaryLength();
	}
}
