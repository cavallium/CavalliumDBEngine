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
			Mono<ByteBuf> prefixKeyMono,
			Flux<ByteBuf> debuggingKeysFlux) {
		return Mono.usingWhen(prefixKeyMono,
				prefixKey -> Mono
						.fromSupplier(() -> DatabaseMapDictionary
								.tail(dictionary,
										prefixKey.retain(),
										keySerializer,
										valueSerializer
								)
						)
						.transform(mono -> {
							if (assertsEnabled && enableAssertionsWhenUsingAssertions) {
								return debuggingKeysFlux.handle((key, sink) -> {
									try {
										if (key.readableBytes() != prefixKey.readableBytes() + getKeyBinaryLength()) {
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

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength();
	}
}
