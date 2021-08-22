package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.List;
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

	private final SerializerFixedBinaryLength<T, ByteBuf> keySerializer;
	private final boolean enableAssertionsWhenUsingAssertions;

	public SubStageGetterSet(SerializerFixedBinaryLength<T, ByteBuf> keySerializer,
			boolean enableAssertionsWhenUsingAssertions) {
		this.keySerializer = keySerializer;
		this.enableAssertionsWhenUsingAssertions = enableAssertionsWhenUsingAssertions;
	}

	@Override
	public Mono<DatabaseSetDictionary<T>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<ByteBuf> prefixKeyMono,
			Flux<ByteBuf> debuggingKeysFlux) {
		return Mono.usingWhen(prefixKeyMono,
				prefixKey -> Mono
						.fromSupplier(() -> DatabaseSetDictionary.tail(dictionary, prefixKey.retain(), keySerializer))
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
