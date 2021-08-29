package it.cavallium.dbengine.database.collections;

import io.netty.buffer.api.Buffer;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
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

@SuppressWarnings("unused")
public class DatabaseSingleMapped<A, B> implements DatabaseStageEntry<A> {

	private final DatabaseStageEntry<B> serializedSingle;
	private final Serializer<A, B> serializer;

	public DatabaseSingleMapped(DatabaseStageEntry<B> serializedSingle, Serializer<A, B> serializer) {
		this.serializedSingle = serializedSingle;
		this.serializer = serializer;
	}

	private void deserializeSink(B value, SynchronousSink<A> sink) {
		try {
			sink.next(this.deserialize(value));
		} catch (SerializationException ex) {
			sink.error(ex);
		}
	}

	@Override
	public Mono<A> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return serializedSingle.get(snapshot, existsAlmostCertainly).handle(this::deserializeSink);
	}

	@Override
	public Mono<A> getOrDefault(@Nullable CompositeSnapshot snapshot, Mono<A> defaultValue) {
		return serializedSingle.get(snapshot).handle(this::deserializeSink).switchIfEmpty(defaultValue);
	}

	@Override
	public Mono<Void> set(A value) {
		return Mono
				.fromCallable(() -> serialize(value))
				.flatMap(serializedSingle::set);
	}

	@Override
	public Mono<A> setAndGetPrevious(A value) {
		return Mono
				.fromCallable(() -> serialize(value))
				.flatMap(serializedSingle::setAndGetPrevious)
				.handle(this::deserializeSink);
	}

	@Override
	public Mono<Boolean> setAndGetChanged(A value) {
		return Mono
				.fromCallable(() -> serialize(value))
				.flatMap(serializedSingle::setAndGetChanged)
				.single();
	}

	@Override
	public Mono<A> update(SerializationFunction<@Nullable A, @Nullable A> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return serializedSingle.update(oldValue -> {
			var result = updater.apply(oldValue == null ? null : this.deserialize(oldValue));
			if (result == null) {
				return null;
			} else {
				return this.serialize(result);
			}
		}, updateReturnMode, existsAlmostCertainly).handle(this::deserializeSink);
	}

	@Override
	public Mono<Delta<A>> updateAndGetDelta(SerializationFunction<@Nullable A, @Nullable A> updater,
			boolean existsAlmostCertainly) {
		return serializedSingle.updateAndGetDelta(oldValue -> {
			var result = updater.apply(oldValue == null ? null : this.deserialize(oldValue));
			if (result == null) {
				return null;
			} else {
				return this.serialize(result);
			}
		}, existsAlmostCertainly).transform(mono -> LLUtils.mapDelta(mono, this::deserialize));
	}

	@Override
	public Mono<Void> clear() {
		return serializedSingle.clear();
	}

	@Override
	public Mono<A> clearAndGetPrevious() {
		return serializedSingle.clearAndGetPrevious().handle(this::deserializeSink);
	}

	@Override
	public Mono<Boolean> clearAndGetStatus() {
		return serializedSingle.clearAndGetStatus();
	}

	@Override
	public Mono<Void> close() {
		return serializedSingle.close();
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return serializedSingle.leavesCount(snapshot, fast);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return serializedSingle.isEmpty(snapshot);
	}

	@Override
	public DatabaseStageEntry<A> entry() {
		return this;
	}

	@Override
	public Flux<BadBlock> badBlocks() {
		return this.serializedSingle.badBlocks();
	}

	@Override
	public void release() {
		serializedSingle.release();
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private A deserialize(B bytes) throws SerializationException {
		return serializer.deserialize(bytes);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private B serialize(A bytes) throws SerializationException {
		return serializer.serialize(bytes);
	}
}
