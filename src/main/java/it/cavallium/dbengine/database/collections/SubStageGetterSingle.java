package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.Arrays;
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

	private final Serializer<T, byte[]> serializer;

	public SubStageGetterSingle(Serializer<T, byte[]> serializer) {
		this.serializer = serializer;
	}

	@Override
	public Mono<DatabaseStageEntry<T>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] keyPrefix,
			Flux<byte[]> debuggingKeyFlux) {
		return debuggingKeyFlux
				.singleOrEmpty()
				.flatMap(key -> Mono
						.<DatabaseStageEntry<T>>fromCallable(() -> {
							if (!Arrays.equals(keyPrefix, key)) {
								throw new IndexOutOfBoundsException("Found more than one element!");
							}
							return null;
						})
				)
				.then(Mono.fromSupplier(() -> new DatabaseSingle<>(dictionary, keyPrefix, serializer)));
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
