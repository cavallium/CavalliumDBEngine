package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.Send;
import io.netty5.buffer.api.internal.ResourceSupport;
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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

public class DatabaseMapSingle<U> extends ResourceSupport<DatabaseStage<U>, DatabaseMapSingle<U>> implements
		DatabaseStageEntry<U> {

	private static final Logger LOG = LogManager.getLogger(DatabaseMapSingle.class);

	private final AtomicLong totalZeroBytesErrors = new AtomicLong();
	private static final Drop<DatabaseMapSingle<?>> DROP = new Drop<>() {
		@Override
		public void drop(DatabaseMapSingle<?> obj) {
			try {
				obj.key.close();
			} catch (Throwable ex) {
				LOG.error("Failed to close key", ex);
			}
			if (obj.onClose != null) {
				obj.onClose.run();
			}
		}

		@Override
		public Drop<DatabaseMapSingle<?>> fork() {
			return this;
		}

		@Override
		public void attach(DatabaseMapSingle<?> obj) {

		}
	};

	private final LLDictionary dictionary;
	private final Mono<Send<Buffer>> keyMono;
	private final Serializer<U> serializer;

	private Buffer key;
	private Runnable onClose;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public DatabaseMapSingle(LLDictionary dictionary, Buffer key, Serializer<U> serializer,
			Runnable onClose) {
		super((Drop<DatabaseMapSingle<U>>) (Drop) DROP);
		this.dictionary = dictionary;
		this.key = key;
		this.keyMono = LLUtils.lazyRetain(this.key);
		this.serializer = serializer;
		this.onClose = onClose;
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
		} catch (IndexOutOfBoundsException ex) {
			var exMessage = ex.getMessage();
			if (exMessage != null && exMessage.contains("read 0 to 0, write 0 to ")) {
				LOG.error("Unexpected zero-bytes value at "
						+ dictionary.getDatabaseName() + ":" + dictionary.getColumnName() + ":" + LLUtils.toStringSafe(key));
				sink.complete();
			} else {
				sink.error(ex);
			}
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
				.get(resolveSnapshot(snapshot), keyMono)
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
			UpdateReturnMode updateReturnMode) {
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
							return serializeValue(result).receive();
						}
					}
				}, updateReturnMode)
				.handle(this::deserializeValue);
	}

	@Override
	public Mono<Delta<U>> updateAndGetDelta(SerializationFunction<@Nullable U, @Nullable U> updater) {
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
							return serializeValue(result).receive();
						}
					}
				}).transform(mono -> LLUtils.mapLLDelta(mono, serialized -> {
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
				.isRangeEmpty(resolveSnapshot(snapshot), keyMono.map(LLRange::single).map(ResourceSupport::send), false)
				.map(empty -> empty ? 0L : 1L);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot), keyMono.map(LLRange::single).map(ResourceSupport::send), true);
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
	protected Owned<DatabaseMapSingle<U>> prepareSend() {
		var keySend = this.key.send();
		var onClose = this.onClose;
		return drop -> {
			var key = keySend.receive();
			var instance = new DatabaseMapSingle<>(dictionary, key, serializer, onClose);
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