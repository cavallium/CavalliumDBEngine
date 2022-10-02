package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.Drop;
import io.netty5.buffer.Owned;
import io.netty5.buffer.internal.ResourceSupport;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.cavallium.dbengine.utils.SimpleResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

public class DatabaseSingleton<U> extends SimpleResource implements DatabaseStageEntry<U> {

	private static final Logger LOG = LogManager.getLogger(DatabaseSingleton.class);

	private final LLSingleton singleton;
	private final Serializer<U> serializer;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public DatabaseSingleton(LLSingleton singleton, Serializer<U> serializer) {
		this.singleton = singleton;
		this.serializer = serializer;
	}

	private LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(singleton);
		}
	}

	private U deserializeValue(Buffer value) {
		try {
			U deserializedValue;
			try (value) {
				deserializedValue = serializer.deserialize(value);
			}
			return deserializedValue;
		} catch (IndexOutOfBoundsException ex) {
			var exMessage = ex.getMessage();
			if (exMessage != null && exMessage.contains("read 0 to 0, write 0 to ")) {
				LOG.error("Unexpected zero-bytes value at " + singleton.getDatabaseName()
						+ ":" + singleton.getColumnName() + ":" + singleton.getName());
				return null;
			} else {
				throw ex;
			}
		}
	}

	private Buffer serializeValue(U value) throws SerializationException {
		var valSizeHint = serializer.getSerializedSizeHint();
		if (valSizeHint == -1) valSizeHint = 128;
		var valBuf = singleton.getAllocator().allocate(valSizeHint);
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
		var resultMono = singleton.get(resolveSnapshot(snapshot));
		return Mono.usingWhen(resultMono,
				result -> Mono.fromSupplier(() -> this.deserializeValue(result)),
				LLUtils::finalizeResource
		);
	}

	@Override
	public Mono<Void> set(U value) {
		return singleton.set(Mono.fromCallable(() -> serializeValue(value)));
	}

	@Override
	public Mono<U> setAndGetPrevious(U value) {
		var resultMono = Flux
				.concat(singleton.get(null), singleton.set(Mono.fromCallable(() -> serializeValue(value))).then(Mono.empty()))
				.last();
		return Mono.usingWhen(resultMono,
				result -> Mono.fromSupplier(() -> this.deserializeValue(result)),
				LLUtils::finalizeResource
		);
	}

	@Override
	public Mono<U> update(SerializationFunction<@Nullable U, @Nullable U> updater,
			UpdateReturnMode updateReturnMode) {
		var resultMono = singleton
			.update((oldValueSer) -> {
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
				result -> Mono.fromSupplier(() -> this.deserializeValue(result)),
				LLUtils::finalizeResource
		);
	}

	@Override
	public Mono<Delta<U>> updateAndGetDelta(SerializationFunction<@Nullable U, @Nullable U> updater) {
		return singleton
				.updateAndGetDelta((oldValueSer) -> {
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
				}).transform(mono -> LLUtils.mapLLDelta(mono, serialized -> serializer.deserialize(serialized)));
	}

	@Override
	public Mono<Void> clear() {
		return singleton.set(Mono.empty());
	}

	@Override
	public Mono<U> clearAndGetPrevious() {
		var resultMono = Flux.concat(singleton.get(null), singleton.set(Mono.empty()).then(Mono.empty())).last();
		return Mono.usingWhen(resultMono,
				result -> Mono.fromSupplier(() -> this.deserializeValue(result)),
				LLUtils::finalizeResource
		);
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return singleton.get(null).map(unused -> 1L).defaultIfEmpty(0L);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return singleton.get(null).map(t -> false).defaultIfEmpty(true);
	}

	@Override
	public Flux<BadBlock> badBlocks() {
		return Flux.empty();
	}

	@Override
	protected void onClose() {

	}
}