package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.Send;
import io.netty.buffer.api.internal.ResourceSupport;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

public class DatabaseSingle<U> implements DatabaseStageEntry<U> {

	private final LLDictionary dictionary;
	private final Buffer key;
	private final Mono<Send<Buffer>> keyMono;
	private final Serializer<U> serializer;

	public DatabaseSingle(LLDictionary dictionary, Send<Buffer> key, Serializer<U> serializer) {
		try (key) {
			this.dictionary = dictionary;
			this.key = key.receive();
			this.keyMono = LLUtils.lazyRetain(this.key);
			this.serializer = serializer;
		}
	}

	private LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	private void deserializeValue(Send<Buffer> value, SynchronousSink<U> sink) {
		try {
			sink.next(serializer.deserialize(value).deserializedData());
		} catch (SerializationException ex) {
			sink.error(ex);
		}
	}

	@Override
	public Mono<U> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return dictionary
				.get(resolveSnapshot(snapshot), keyMono, existsAlmostCertainly)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<U> setAndGetPrevious(U value) {
		return dictionary
				.put(keyMono, Mono.fromCallable(() -> serializer.serialize(value)), LLDictionaryResultType.PREVIOUS_VALUE)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<U> update(SerializationFunction<@Nullable U, @Nullable U> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return dictionary
				.update(keyMono, (oldValueSer) -> {
					var result = updater.apply(
							oldValueSer == null ? null : serializer.deserialize(oldValueSer).deserializedData());
					if (result == null) {
						return null;
					} else {
						return serializer.serialize(result);
					}
				}, updateReturnMode, existsAlmostCertainly)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<Delta<U>> updateAndGetDelta(SerializationFunction<@Nullable U, @Nullable U> updater,
			boolean existsAlmostCertainly) {
		return dictionary
				.updateAndGetDelta(keyMono, (oldValueSer) -> {
					var result = updater.apply(
							oldValueSer == null ? null : serializer.deserialize(oldValueSer).deserializedData());
					if (result == null) {
						return null;
					} else {
						return serializer.serialize(result);
					}
				}, existsAlmostCertainly).transform(mono -> LLUtils.mapLLDelta(mono,
						serialized -> serializer.deserialize(serialized).deserializedData()
				));
	}

	@Override
	public Mono<U> clearAndGetPrevious() {
		return dictionary
				.remove(keyMono, LLDictionaryResultType.PREVIOUS_VALUE)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot), keyMono.map(LLRange::single).map(ResourceSupport::send))
				.map(empty -> empty ? 0L : 1L);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot), keyMono.map(LLRange::single).map(ResourceSupport::send));
	}

	@Override
	public void release() {
		key.close();
	}

	@Override
	public Flux<BadBlock> badBlocks() {
		return dictionary.badBlocks(keyMono.map(LLRange::single).map(ResourceSupport::send));
	}
}