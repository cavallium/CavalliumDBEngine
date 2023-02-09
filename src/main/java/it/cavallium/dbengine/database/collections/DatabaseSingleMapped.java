package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.Mapper;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

public class DatabaseSingleMapped<A, B> implements DatabaseStageEntry<A> {

	private final Mapper<A, B> mapper;

	private final DatabaseStageEntry<B> serializedSingle;

	public DatabaseSingleMapped(DatabaseStageEntry<B> serializedSingle, Mapper<A, B> mapper) {
		this.serializedSingle = serializedSingle;
		this.mapper = mapper;
	}

	private DatabaseSingleMapped(DatabaseStage<B> serializedSingle, Mapper<A, B> mapper) {
		this.mapper = mapper;

		this.serializedSingle = (DatabaseStageEntry<B>) serializedSingle;
	}

	@Override
	public A get(@Nullable CompositeSnapshot snapshot) {
		var data = serializedSingle.get(snapshot);
		if (data == null) return null;
		return this.unMap(data);
	}

	@Override
	public A getOrDefault(@Nullable CompositeSnapshot snapshot, A defaultValue) {
		var value = serializedSingle.get(snapshot);
		if (value == null) return defaultValue;
		return this.unMap(value);
	}

	@Override
	public void set(A value) {
		B mappedValue = value != null ? map(value) : null;
		serializedSingle.set(mappedValue);
	}

	@Override
	public A setAndGetPrevious(A value) {
		var mappedValue = value != null ? map(value) : null;
		var prev = serializedSingle.setAndGetPrevious(mappedValue);
		return prev != null ? unMap(prev) : null;
	}

	@Override
	public boolean setAndGetChanged(A value) {
		var mappedValue = value != null ? map(value) : null;
		return serializedSingle.setAndGetChanged(mappedValue);
	}

	@Override
	public A update(SerializationFunction<@Nullable A, @Nullable A> updater, UpdateReturnMode updateReturnMode) {
		B prev = serializedSingle.update(oldValue -> {
			var result = updater.apply(oldValue == null ? null : this.unMap(oldValue));
			if (result == null) {
				return null;
			} else {
				return this.map(result);
			}
		}, updateReturnMode);
		return prev != null ? unMap(prev) : null;
	}

	@Override
	public Delta<A> updateAndGetDelta(SerializationFunction<@Nullable A, @Nullable A> updater) {
		var delta = serializedSingle.updateAndGetDelta(oldValue -> {
			var result = updater.apply(oldValue == null ? null : this.unMap(oldValue));
			if (result == null) {
				return null;
			} else {
				return this.map(result);
			}
		});
		return LLUtils.mapDelta(delta, this::unMap);
	}

	@Override
	public void clear() {
		serializedSingle.clear();
	}

	@Override
	public A clearAndGetPrevious() {
		var prev = serializedSingle.clearAndGetPrevious();
		return prev != null ? unMap(prev) : null;
	}

	@Override
	public boolean clearAndGetStatus() {
		return serializedSingle.clearAndGetStatus();
	}

	@Override
	public long leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return serializedSingle.leavesCount(snapshot, fast);
	}

	@Override
	public boolean isEmpty(@Nullable CompositeSnapshot snapshot) {
		return serializedSingle.isEmpty(snapshot);
	}

	@Override
	public DatabaseStageEntry<A> entry() {
		return this;
	}

	@Override
	public Stream<BadBlock> badBlocks() {
		return this.serializedSingle.badBlocks();
	}

	private A unMap(B bytes) throws SerializationException {
		return mapper.unmap(bytes);
	}

	private B map(A bytes) throws SerializationException {
		return mapper.map(bytes);
	}
}
