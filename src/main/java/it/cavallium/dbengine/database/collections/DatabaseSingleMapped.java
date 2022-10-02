package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Drop;
import io.netty5.buffer.Owned;
import io.netty5.util.Send;
import io.netty5.buffer.internal.ResourceSupport;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.Mapper;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.utils.SimpleResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

@SuppressWarnings("unused")
public class DatabaseSingleMapped<A, B> extends SimpleResource implements DatabaseStageEntry<A> {

	private static final Logger logger = LogManager.getLogger(DatabaseSingleMapped.class);

	private final Mapper<A, B> mapper;

	private final DatabaseStageEntry<B> serializedSingle;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public DatabaseSingleMapped(DatabaseStageEntry<B> serializedSingle, Mapper<A, B> mapper,
			Drop<DatabaseSingleMapped<A, B>> drop) {
		this.serializedSingle = serializedSingle;
		this.mapper = mapper;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private DatabaseSingleMapped(DatabaseStage<B> serializedSingle, Mapper<A, B> mapper,
			Drop<DatabaseSingleMapped<A, B>> drop) {
		this.mapper = mapper;

		this.serializedSingle = (DatabaseStageEntry<B>) serializedSingle;
	}

	private void deserializeSink(B value, SynchronousSink<A> sink) {
		try {
			sink.next(this.unMap(value));
		} catch (SerializationException ex) {
			sink.error(ex);
		}
	}

	@Override
	public Mono<A> get(@Nullable CompositeSnapshot snapshot) {
		return serializedSingle.get(snapshot).handle((value, sink) -> deserializeSink(value, sink));
	}

	@Override
	public Mono<A> getOrDefault(@Nullable CompositeSnapshot snapshot, Mono<A> defaultValue) {
		return serializedSingle.get(snapshot).handle((B value, SynchronousSink<A> sink) -> deserializeSink(value, sink)).switchIfEmpty(defaultValue);
	}

	@Override
	public Mono<Void> set(A value) {
		return Mono
				.fromCallable(() -> map(value))
				.flatMap(value1 -> serializedSingle.set(value1));
	}

	@Override
	public Mono<A> setAndGetPrevious(A value) {
		return Mono
				.fromCallable(() -> map(value))
				.flatMap(value2 -> serializedSingle.setAndGetPrevious(value2))
				.handle((value1, sink) -> deserializeSink(value1, sink));
	}

	@Override
	public Mono<Boolean> setAndGetChanged(A value) {
		return Mono
				.fromCallable(() -> map(value))
				.flatMap(value1 -> serializedSingle.setAndGetChanged(value1))
				.single();
	}

	@Override
	public Mono<A> update(SerializationFunction<@Nullable A, @Nullable A> updater,
			UpdateReturnMode updateReturnMode) {
		return serializedSingle.update(oldValue -> {
			var result = updater.apply(oldValue == null ? null : this.unMap(oldValue));
			if (result == null) {
				return null;
			} else {
				return this.map(result);
			}
		}, updateReturnMode).handle((value, sink) -> deserializeSink(value, sink));
	}

	@Override
	public Mono<Delta<A>> updateAndGetDelta(SerializationFunction<@Nullable A, @Nullable A> updater) {
		return serializedSingle.updateAndGetDelta(oldValue -> {
			var result = updater.apply(oldValue == null ? null : this.unMap(oldValue));
			if (result == null) {
				return null;
			} else {
				return this.map(result);
			}
		}).transform(mono -> LLUtils.mapDelta(mono, bytes -> unMap(bytes)));
	}

	@Override
	public Mono<Void> clear() {
		return serializedSingle.clear();
	}

	@Override
	public Mono<A> clearAndGetPrevious() {
		return serializedSingle.clearAndGetPrevious().handle((value, sink) -> deserializeSink(value, sink));
	}

	@Override
	public Mono<Boolean> clearAndGetStatus() {
		return serializedSingle.clearAndGetStatus();
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

	private A unMap(B bytes) throws SerializationException {
		return mapper.unmap(bytes);
	}

	private B map(A bytes) throws SerializationException {
		return mapper.map(bytes);
	}

	@Override
	protected void onClose() {
		serializedSingle.close();
	}
}
