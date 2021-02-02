package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.Arrays;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterSingle<T> implements SubStageGetter<T, DatabaseStageEntry<T>> {

	private final Serializer<T, byte[]> serializer;

	public SubStageGetterSingle(Serializer<T, byte[]> serializer) {
		this.serializer = serializer;
	}

	@Override
	public Mono<DatabaseStageEntry<T>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] keyPrefix,
			Flux<byte[]> keyFlux) {
		//System.out.println(Thread.currentThread() + "subStageGetterSingle1");
		return keyFlux
				.singleOrEmpty()
				.flatMap(key -> Mono
						.<DatabaseStageEntry<T>>fromCallable(() -> {
							//System.out.println(Thread.currentThread() + "subStageGetterSingle2");
							if (!Arrays.equals(keyPrefix, key)) {
								throw new IndexOutOfBoundsException("Found more than one element!");
							}
							return null;
						})
				)
				.then(Mono.fromSupplier(() -> {
					//System.out.println(Thread.currentThread() + "subStageGetterSingle3");
					return new DatabaseSingle<T>(dictionary,
							keyPrefix,
							serializer
					);
				}));
	}

	@Override
	public boolean needsKeyFlux() {
		return true;
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private T deserialize(byte[] bytes) {
		return serializer.deserialize(bytes);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private byte[] serialize(T bytes) {
		return serializer.serialize(bytes);
	}
}
