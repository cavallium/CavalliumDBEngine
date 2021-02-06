package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class DatabaseSingleMapped<U> implements DatabaseStageEntry<U> {

	private final DatabaseSingle<byte[]> serializedSingle;
	private final Serializer<U, byte[]> serializer;

	public DatabaseSingleMapped(DatabaseSingle<byte[]> serializedSingle, Serializer<U, byte[]> serializer) {
		this.serializedSingle = serializedSingle;
		this.serializer = serializer;
	}

	@Override
	public Mono<U> get(@Nullable CompositeSnapshot snapshot) {
		return serializedSingle.get(snapshot).map(this::deserialize);
	}

	@Override
	public Mono<U> getOrDefault(@Nullable CompositeSnapshot snapshot, Mono<U> defaultValue) {
		return serializedSingle.get(snapshot).map(this::deserialize).switchIfEmpty(defaultValue);
	}

	@Override
	public Mono<Void> set(U value) {
		return serializedSingle.set(serialize(value));
	}

	@Override
	public Mono<U> setAndGetPrevious(U value) {
		return serializedSingle.setAndGetPrevious(serialize(value)).map(this::deserialize);
	}

	@Override
	public Mono<Boolean> setAndGetStatus(U value) {
		return serializedSingle.setAndGetStatus(serialize(value));
	}

	@Override
	public Mono<Void> update(Function<Optional<U>, Optional<U>> updater) {
		return serializedSingle.update(oldValue -> updater.apply(oldValue.map(this::deserialize)).map(this::serialize));
	}

	@Override
	public Mono<Void> clear() {
		return serializedSingle.clear();
	}

	@Override
	public Mono<U> clearAndGetPrevious() {
		return serializedSingle.clearAndGetPrevious().map(this::deserialize);
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
	public Mono<Long> size(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return serializedSingle.size(snapshot, fast);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return serializedSingle.isEmpty(snapshot);
	}

	@Override
	public DatabaseStageEntry<U> entry() {
		return this;
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private U deserialize(byte[] bytes) {
		return serializer.deserialize(bytes);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private byte[] serialize(U bytes) {
		return serializer.serialize(bytes);
	}
}
