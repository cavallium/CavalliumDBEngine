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

	public SubStageGetterSet(SerializerFixedBinaryLength<T, ByteBuf> keySerializer) {
		this.keySerializer = keySerializer;
	}

	@Override
	public Mono<DatabaseSetDictionary<T>> subStage(LLDictionary dictionary,
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
							.fromSupplier(() -> DatabaseSetDictionary
									.tail(
											dictionary,
											prefixKey.retain(),
											keySerializer
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
		try {
			return Mono
					.<Void>fromCallable(() -> {
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
					})
					.doFirst(prefixKey::retain)
					.doAfterTerminate(prefixKey::release);
		} finally {
			prefixKey.release();
		}
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength();
	}
}
