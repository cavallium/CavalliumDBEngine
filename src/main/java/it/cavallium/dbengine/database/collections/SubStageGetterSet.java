package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Resource;
import io.netty5.buffer.api.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.collections.DatabaseEmpty.Nothing;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class SubStageGetterSet<T> implements
		SubStageGetter<Object2ObjectSortedMap<T, Nothing>, DatabaseSetDictionary<T>> {

	private final SerializerFixedBinaryLength<T> keySerializer;

	public SubStageGetterSet(SerializerFixedBinaryLength<T> keySerializer) {
		this.keySerializer = keySerializer;
	}

	@Override
	public Mono<DatabaseSetDictionary<T>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<Buffer> prefixKeyMono) {
		return prefixKeyMono.map(prefixKey -> DatabaseSetDictionary.tail(dictionary,
				BufSupplier.ofOwned(prefixKey),
				keySerializer,
				null
		));
	}

	public int getKeyBinaryLength() {
		return keySerializer.getSerializedBinaryLength();
	}
}
