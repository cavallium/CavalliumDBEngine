package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterMapDeep<T, U, US extends DatabaseStage<U>> implements
		SubStageGetter<Map<T, U>, DatabaseMapDictionaryDeep<T, U, US>> {

	private final SubStageGetter<U, US> subStageGetter;
	private final SerializerFixedBinaryLength<T, Buffer> keySerializer;
	private final int keyExtLength;

	public SubStageGetterMapDeep(SubStageGetter<U, US> subStageGetter,
			SerializerFixedBinaryLength<T, Buffer> keySerializer,
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
			Mono<Buffer> prefixKeyMono) {
		return Mono.usingWhen(prefixKeyMono,
				prefixKey -> Mono
						.fromSupplier(() -> DatabaseMapDictionaryDeep
								.deepIntermediate(dictionary,
										prefixKey.retain(),
										keySerializer,
										subStageGetter,
										keyExtLength
								)
						),
				prefixKey -> Mono.fromRunnable(prefixKey::release)
		);
	}

	@Override
	public boolean isMultiKey() {
		return true;
	}

	private Mono<Void> checkKeyFluxConsistency(Buffer prefixKey, List<Buffer> keys) {
		return Mono
				.fromCallable(() -> {
					try {
						for (Buffer key : keys) {
							assert key.readableBytes() == prefixKey.readableBytes() + getKeyBinaryLength();
						}
					} finally {
						prefixKey.release();
						for (Buffer key : keys) {
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
