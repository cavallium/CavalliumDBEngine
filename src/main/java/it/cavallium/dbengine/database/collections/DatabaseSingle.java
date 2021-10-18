package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.SearchResultKeys;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.database.serialization.Serializer;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

public class DatabaseSingle<U> extends ResourceSupport<DatabaseStage<U>, DatabaseSingle<U>> implements
		DatabaseStageEntry<U> {

	private static final Logger logger = LoggerFactory.getLogger(DatabaseSingle.class);

	private static final Drop<DatabaseSingle<?>> DROP = new Drop<>() {
		@Override
		public void drop(DatabaseSingle<?> obj) {
			try {
				obj.key.close();
			} catch (Throwable ex) {
				logger.error("Failed to close key", ex);
			}
			if (obj.onClose != null) {
				obj.onClose.run();
			}
		}

		@Override
		public Drop<DatabaseSingle<?>> fork() {
			return this;
		}

		@Override
		public void attach(DatabaseSingle<?> obj) {

		}
	};

	private final LLDictionary dictionary;
	private final Mono<Send<Buffer>> keyMono;
	private final Serializer<U> serializer;

	private Buffer key;
	private Runnable onClose;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public DatabaseSingle(LLDictionary dictionary, Send<Buffer> key, Serializer<U> serializer,
			Runnable onClose) {
		super((Drop<DatabaseSingle<U>>) (Drop) DROP);
		try (key) {
			this.dictionary = dictionary;
			this.key = key.receive();
			this.keyMono = LLUtils.lazyRetain(this.key);
			this.serializer = serializer;
			this.onClose = onClose;
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
			U deserializedValue;
			try (var valueBuf = value.receive()) {
				deserializedValue = serializer.deserialize(valueBuf);
			}
			sink.next(deserializedValue);
		} catch (SerializationException ex) {
			sink.error(ex);
		}
	}

	private Send<Buffer> serializeValue(U value) throws SerializationException {
		var valSizeHint = serializer.getSerializedSizeHint();
		if (valSizeHint == -1) valSizeHint = 128;
		try (var valBuf = dictionary.getAllocator().allocate(valSizeHint)) {
			serializer.serialize(value, valBuf);
			return valBuf.send();
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
				.put(keyMono, Mono.fromCallable(() -> serializeValue(value)), LLDictionaryResultType.PREVIOUS_VALUE)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<U> update(SerializationFunction<@Nullable U, @Nullable U> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return dictionary
				.update(keyMono, (oldValueSer) -> {
					try (oldValueSer) {
						U result;
						if (oldValueSer == null) {
							result = updater.apply(null);
						} else {
							U deserializedValue;
							try (var valueBuf = oldValueSer.receive()) {
								deserializedValue = serializer.deserialize(valueBuf);
							}
							result = updater.apply(deserializedValue);
						}
						if (result == null) {
							return null;
						} else {
							return serializeValue(result);
						}
					}
				}, updateReturnMode, existsAlmostCertainly)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<Delta<U>> updateAndGetDelta(SerializationFunction<@Nullable U, @Nullable U> updater,
			boolean existsAlmostCertainly) {
		return dictionary
				.updateAndGetDelta(keyMono, (oldValueSer) -> {
					try (oldValueSer) {
						U result;
						if (oldValueSer == null) {
							result = updater.apply(null);
						} else {
							U deserializedValue;
							try (var valueBuf = oldValueSer.receive()) {
								deserializedValue = serializer.deserialize(valueBuf);
							}
							result = updater.apply(deserializedValue);
						}
						if (result == null) {
							return null;
						} else {
							return serializeValue(result);
						}
					}
				}, existsAlmostCertainly).transform(mono -> LLUtils.mapLLDelta(mono, serialized -> {
					try (var valueBuf = serialized.receive()) {
						return serializer.deserialize(valueBuf);
					}
				}));
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
	public Flux<BadBlock> badBlocks() {
		return dictionary.badBlocks(keyMono.map(LLRange::single).map(ResourceSupport::send));
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		throw new IllegalStateException("Closed");
	}

	@Override
	protected Owned<DatabaseSingle<U>> prepareSend() {
		var key = this.key.send();
		var onClose = this.onClose;
		return drop -> {
			var instance = new DatabaseSingle<>(dictionary, key, serializer, onClose);
			drop.attach(instance);
			return instance;
		};
	}

	@Override
	protected void makeInaccessible() {
		this.key = null;
		this.onClose = null;
	}
}