package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.Mapper;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

@SuppressWarnings("unused")
public class DatabaseSingleMapped<A, B> extends ResourceSupport<DatabaseStage<A>, DatabaseSingleMapped<A, B>>
		implements DatabaseStageEntry<A> {

	private static final Logger logger = LoggerFactory.getLogger(DatabaseSingleMapped.class);

	private static final Drop<DatabaseSingleMapped<?, ?>> DROP = new Drop<>() {
		@Override
		public void drop(DatabaseSingleMapped<?, ?> obj) {
			try {
				if (obj.serializedSingle != null) {
					obj.serializedSingle.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close serializedSingle", ex);
			}
		}

		@Override
		public Drop<DatabaseSingleMapped<?, ?>> fork() {
			return this;
		}

		@Override
		public void attach(DatabaseSingleMapped<?, ?> obj) {

		}
	};

	private final Mapper<A, B> mapper;

	private DatabaseStageEntry<B> serializedSingle;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public DatabaseSingleMapped(DatabaseStageEntry<B> serializedSingle, Mapper<A, B> mapper,
			Drop<DatabaseSingleMapped<A, B>> drop) {
		super((Drop<DatabaseSingleMapped<A,B>>) (Drop) DROP);
		this.serializedSingle = serializedSingle;
		this.mapper = mapper;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private DatabaseSingleMapped(Send<DatabaseStage<B>> serializedSingle, Mapper<A, B> mapper,
			Drop<DatabaseSingleMapped<A, B>> drop) {
		super((Drop<DatabaseSingleMapped<A,B>>) (Drop) DROP);
		this.mapper = mapper;

		this.serializedSingle = (DatabaseStageEntry<B>) serializedSingle.receive();
	}

	private void deserializeSink(B value, SynchronousSink<A> sink) {
		try {
			sink.next(this.unMap(value));
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
				.fromCallable(() -> map(value))
				.flatMap(serializedSingle::set);
	}

	@Override
	public Mono<A> setAndGetPrevious(A value) {
		return Mono
				.fromCallable(() -> map(value))
				.flatMap(serializedSingle::setAndGetPrevious)
				.handle(this::deserializeSink);
	}

	@Override
	public Mono<Boolean> setAndGetChanged(A value) {
		return Mono
				.fromCallable(() -> map(value))
				.flatMap(serializedSingle::setAndGetChanged)
				.single();
	}

	@Override
	public Mono<A> update(SerializationFunction<@Nullable A, @Nullable A> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return serializedSingle.update(oldValue -> {
			var result = updater.apply(oldValue == null ? null : this.unMap(oldValue));
			if (result == null) {
				return null;
			} else {
				return this.map(result);
			}
		}, updateReturnMode, existsAlmostCertainly).handle(this::deserializeSink);
	}

	@Override
	public Mono<Delta<A>> updateAndGetDelta(SerializationFunction<@Nullable A, @Nullable A> updater,
			boolean existsAlmostCertainly) {
		return serializedSingle.updateAndGetDelta(oldValue -> {
			var result = updater.apply(oldValue == null ? null : this.unMap(oldValue));
			if (result == null) {
				return null;
			} else {
				return this.map(result);
			}
		}, existsAlmostCertainly).transform(mono -> LLUtils.mapDelta(mono, this::unMap));
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

	//todo: temporary wrapper. convert the whole class to buffers
	private A unMap(B bytes) throws SerializationException {
		return mapper.unmap(bytes);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private B map(A bytes) throws SerializationException {
		return mapper.map(bytes);
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		throw new IllegalStateException("Closed");
	}

	@Override
	protected Owned<DatabaseSingleMapped<A, B>> prepareSend() {
		var serializedSingle = this.serializedSingle.send();
		return drop -> new DatabaseSingleMapped<>(serializedSingle, mapper, drop);
	}

	@Override
	protected void makeInaccessible() {
		this.serializedSingle = null;
	}

}
