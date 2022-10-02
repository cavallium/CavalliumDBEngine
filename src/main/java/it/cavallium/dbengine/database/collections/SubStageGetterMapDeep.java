package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Buffer;
import io.netty5.util.Resource;
import io.netty5.util.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class SubStageGetterMapDeep<T, U, US extends DatabaseStage<U>> implements
		SubStageGetter<Object2ObjectSortedMap<T, U>, DatabaseMapDictionaryDeep<T, U, US>> {

	private final SubStageGetter<U, US> subStageGetter;
	private final SerializerFixedBinaryLength<T> keySerializer;
	private final int keyExtLength;

	public SubStageGetterMapDeep(SubStageGetter<U, US> subStageGetter,
			SerializerFixedBinaryLength<T> keySerializer,
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
		return prefixKeyMono.map(prefixKey -> DatabaseMapDictionaryDeep.deepIntermediate(dictionary,
				BufSupplier.ofOwned(prefixKey),
				keySerializer,
				subStageGetter,
				keyExtLength
		));
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength() + keyExtLength;
	}
}
