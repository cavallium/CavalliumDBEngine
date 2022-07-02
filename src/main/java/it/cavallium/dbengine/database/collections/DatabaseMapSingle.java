package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.BufSupplier;
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

public class DatabaseMapSingle<U> extends ResourceSupport<DatabaseStage<U>, DatabaseMapSingle<U>> implements
		DatabaseStageEntry<U> {

	private static final Logger LOG = LogManager.getLogger(DatabaseMapSingle.class);

	private final AtomicLong totalZeroBytesErrors = new AtomicLong();
	private static final Drop<DatabaseMapSingle<?>> DROP = new Drop<>() {
		@Override
		public void drop(DatabaseMapSingle<?> obj) {
			if (obj.keySupplier != null) {
				obj.keySupplier.close();
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
	private final Mono<Buffer> keyMono;
	private final Serializer<U> serializer;
	private BufSupplier keySupplier;
	private Runnable onClose;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public DatabaseMapSingle(LLDictionary dictionary, BufSupplier keySupplier, Serializer<U> serializer,
			Runnable onClose) {
		super((Drop<DatabaseMapSingle<U>>) (Drop) DROP);
		this.dictionary = dictionary;
		this.keySupplier = keySupplier;
		this.keyMono = Mono.fromSupplier(() -> keySupplier.get());
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

	private U deserializeValue(Buffer value) {
		try {
			return serializer.deserialize(value);
		} catch (IndexOutOfBoundsException ex) {
			var exMessage = ex.getMessage();
			if (exMessage != null && exMessage.contains("read 0 to 0, write 0 to ")) {
				try (var key = keySupplier.get()) {
					LOG.error("Unexpected zero-bytes value at "
							+ dictionary.getDatabaseName() + ":" + dictionary.getColumnName() + ":" + LLUtils.toStringSafe(key));
				}
				return null;
			} else {
				throw ex;
			}
		} catch (SerializationException ex) {
			throw ex;
		}
	}

	private Buffer serializeValue(U value) throws SerializationException {
		var valSizeHint = serializer.getSerializedSizeHint();
		if (valSizeHint == -1) valSizeHint = 128;
		var valBuf = dictionary.getAllocator().allocate(valSizeHint);
		try {
			serializer.serialize(value, valBuf);
			return valBuf;
		} catch (Throwable ex) {
			valBuf.close();
			throw ex;
		}
	}

	@Override
	public Mono<U> get(@Nullable CompositeSnapshot snapshot) {
		return Mono.usingWhen(dictionary.get(resolveSnapshot(snapshot), keyMono),
				buf -> Mono.fromSupplier(() -> deserializeValue(buf)),
				LLUtils::finalizeResource
		);
	}

	@Override
	public Mono<U> setAndGetPrevious(U value) {
		return Mono.usingWhen(dictionary
						.put(keyMono, Mono.fromCallable(() -> serializeValue(value)), LLDictionaryResultType.PREVIOUS_VALUE),
				buf -> Mono.fromSupplier(() -> deserializeValue(buf)),
				LLUtils::finalizeResource);
	}

	@Override
	public Mono<U> update(SerializationFunction<@Nullable U, @Nullable U> updater,
			UpdateReturnMode updateReturnMode) {
		var resultMono = dictionary
				.update(keyMono, (oldValueSer) -> {
					try (oldValueSer) {
						U result;
						if (oldValueSer == null) {
							result = updater.apply(null);
						} else {
							U deserializedValue = serializer.deserialize(oldValueSer);
							result = updater.apply(deserializedValue);
						}
						if (result == null) {
							return null;
						} else {
							return serializeValue(result);
						}
					}
				}, updateReturnMode);
		return Mono.usingWhen(resultMono,
				result -> Mono.fromSupplier(() -> deserializeValue(result)),
				LLUtils::finalizeResource
		);
	}

	@Override
	public Mono<Delta<U>> updateAndGetDelta(SerializationFunction<@Nullable U, @Nullable U> updater) {
		return dictionary
				.updateAndGetDelta(keyMono, (oldValueSer) -> {
					U result;
					if (oldValueSer == null) {
						result = updater.apply(null);
					} else {
						U deserializedValue = serializer.deserialize(oldValueSer);
						result = updater.apply(deserializedValue);
					}
					if (result == null) {
						return null;
					} else {
						return serializeValue(result);
					}
				}).transform(mono -> LLUtils.mapLLDelta(mono, serialized -> serializer.deserialize(serialized)));
	}

	@Override
	public Mono<U> clearAndGetPrevious() {
		return Mono.usingWhen(dictionary.remove(keyMono, LLDictionaryResultType.PREVIOUS_VALUE),
				result -> Mono.fromSupplier(() -> deserializeValue(result)),
				LLUtils::finalizeResource
		);
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot), keyMono.map(single -> LLRange.singleUnsafe(single)), false)
				.map(empty -> empty ? 0L : 1L);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot), keyMono.map(single -> LLRange.singleUnsafe(single)), true);
	}

	@Override
	public Flux<BadBlock> badBlocks() {
		return dictionary.badBlocks(keyMono.map(single -> LLRange.singleUnsafe(single)));
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		throw new IllegalStateException("Closed");
	}

	@Override
	protected Owned<DatabaseMapSingle<U>> prepareSend() {
		var keySupplier = this.keySupplier;
		var onClose = this.onClose;
		return drop -> {
			var instance = new DatabaseMapSingle<>(dictionary, keySupplier, serializer, onClose);
			drop.attach(instance);
			return instance;
		};
	}

	@Override
	protected void makeInaccessible() {
		this.keySupplier = null;
		this.onClose = null;
	}
}