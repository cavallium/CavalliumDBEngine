package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

@SuppressWarnings("unused")
public class DatabaseSingleMapped<A, B> implements DatabaseStageEntry<A> {

	private final DatabaseStageEntry<B> serializedSingle;
	private final Serializer<A, B> serializer;

	public DatabaseSingleMapped(DatabaseStageEntry<B> serializedSingle, Serializer<A, B> serializer) {
		this.serializedSingle = serializedSingle;
		this.serializer = serializer;
	}

	@Override
	public Mono<A> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return serializedSingle.get(snapshot, existsAlmostCertainly).map(this::deserialize);
	}

	@Override
	public Mono<A> getOrDefault(@Nullable CompositeSnapshot snapshot, Mono<A> defaultValue) {
		return serializedSingle.get(snapshot).map(this::deserialize).switchIfEmpty(defaultValue);
	}

	@Override
	public Mono<Void> set(A value) {
		return serializedSingle.set(serialize(value));
	}

	@Override
	public Mono<A> setAndGetPrevious(A value) {
		return serializedSingle.setAndGetPrevious(serialize(value)).map(this::deserialize);
	}

	@Override
	public Mono<Boolean> setAndGetStatus(A value) {
		return serializedSingle.setAndGetStatus(serialize(value));
	}

	@Override
	public Mono<Boolean> update(Function<Optional<A>, Optional<A>> updater, boolean existsAlmostCertainly) {
		return serializedSingle
				.update(oldValue -> updater.apply(oldValue.map(this::deserialize)).map(this::serialize), existsAlmostCertainly);
	}

	@Override
	public Mono<Void> clear() {
		return serializedSingle.clear();
	}

	@Override
	public Mono<A> clearAndGetPrevious() {
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

	//todo: temporary wrapper. convert the whole class to buffers
	private A deserialize(B bytes) {
		return serializer.deserialize(bytes);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private B serialize(A bytes) {
		return serializer.serialize(bytes);
	}
}
