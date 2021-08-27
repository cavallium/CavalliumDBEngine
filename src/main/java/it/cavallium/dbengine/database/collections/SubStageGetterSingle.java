package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.Arrays;
import java.util.List;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterSingle<T> implements SubStageGetter<T, DatabaseStageEntry<T>> {

	private static final boolean assertsEnabled;
	static {
		boolean assertsEnabledTmp = false;
		//noinspection AssertWithSideEffects
		assert assertsEnabledTmp = true;
		//noinspection ConstantConditions
		assertsEnabled = assertsEnabledTmp;
	}

	private final Serializer<T, ByteBuf> serializer;

	public SubStageGetterSingle(Serializer<T, ByteBuf> serializer) {
		this.serializer = serializer;
	}

	@Override
	public Mono<DatabaseStageEntry<T>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<ByteBuf> keyPrefixMono,
			@Nullable Flux<ByteBuf> debuggingKeysFlux) {
		return Mono.usingWhen(
				keyPrefixMono,
				keyPrefix -> Mono
						.<DatabaseStageEntry<T>>fromSupplier(() -> new DatabaseSingle<>(dictionary, keyPrefix.retain(), serializer))
						.transform(mono -> {
							if (debuggingKeysFlux != null) {
								return debuggingKeysFlux.handle((key, sink) -> {
									try {
										if (needsDebuggingKeyFlux() && !LLUtils.equals(keyPrefix, key)) {
											sink.error(new IndexOutOfBoundsException("Found more than one element!"));
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
				keyPrefix -> Mono.fromRunnable(keyPrefix::release)
		);
	}

	@Override
	public boolean isMultiKey() {
		return false;
	}

	@Override
	public boolean needsDebuggingKeyFlux() {
		return assertsEnabled;
	}

}
