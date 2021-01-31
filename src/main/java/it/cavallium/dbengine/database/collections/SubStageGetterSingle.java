package it.cavallium.dbengine.database.collections;

import io.netty.buffer.Unpooled;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import java.util.Arrays;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SubStageGetterSingle<T> implements SubStageGetter<T, DatabaseStageEntry<T>> {

	private final Serializer<T> serializer;

	public SubStageGetterSingle(Serializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	public Mono<DatabaseStageEntry<T>> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			byte[] keyPrefix,
			Flux<byte[]> keyFlux) {
		return keyFlux.singleOrEmpty().flatMap(key -> Mono.fromCallable(() -> {
			if (!Arrays.equals(keyPrefix, key)) {
				throw new IndexOutOfBoundsException("Found more than one element!");
			}
			return null;
		})).thenReturn(new DatabaseSingle(dictionary, keyPrefix, Serializer.noopBytes()));
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private T deserialize(byte[] bytes) {
		var serialized = Unpooled.wrappedBuffer(bytes);
		return serializer.deserialize(serialized);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private byte[] serialize(T bytes) {
		var output = Unpooled.buffer();
		serializer.serialize(bytes, output);
		output.resetReaderIndex();
		int length = output.readableBytes();
		var outputBytes = new byte[length];
		output.getBytes(0, outputBytes, 0, length);
		return outputBytes;
	}
}
