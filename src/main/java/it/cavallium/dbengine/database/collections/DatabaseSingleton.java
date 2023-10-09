package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.BufDataInput;
import it.cavallium.buffer.BufDataOutput;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.DbProgress;
import it.cavallium.dbengine.client.SSTVerificationProgress;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.disk.CachedSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

public class DatabaseSingleton<U> implements DatabaseStageEntry<U> {

	private static final Logger LOG = LogManager.getLogger(DatabaseSingleton.class);

	private final LLSingleton singleton;
	private final Serializer<U> serializer;

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

	private U deserializeValue(Buf value) {
		if (value == null) {
			return null;
		}
		try {
			return serializer.deserialize(BufDataInput.create(value));
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

	private Buf serializeValue(U value) throws SerializationException {
		var valSizeHint = serializer.getSerializedSizeHint();
		if (valSizeHint == -1) valSizeHint = 128;
		var valBuf = BufDataOutput.create(valSizeHint);
		serializer.serialize(value, valBuf);
		return valBuf.asList();
	}

	@Override
	public U get(@Nullable CompositeSnapshot snapshot) {
		Buf result = singleton.get(resolveSnapshot(snapshot));
		return this.deserializeValue(result);
	}

	@Override
	public void set(U value) {
		singleton.set(serializeValue(value));
	}

	@Override
	public U setAndGetPrevious(U value) {
		var prev = singleton.get(null);
		singleton.set(serializeValue(value));
		return this.deserializeValue(prev);
	}

	@Override
	public U update(SerializationFunction<@Nullable U, @Nullable U> updater, UpdateReturnMode updateReturnMode) {
		var serializedUpdater = new CachedSerializationFunction<>(updater, this::serializeValue, this::deserializeValue);
		singleton.update(serializedUpdater, UpdateReturnMode.NOTHING);
		return serializedUpdater.getResult(updateReturnMode);
	}

	@Override
	public Delta<U> updateAndGetDelta(SerializationFunction<@Nullable U, @Nullable U> updater) {
		var serializedUpdater = new CachedSerializationFunction<>(updater, this::serializeValue, this::deserializeValue);
		singleton.update(serializedUpdater, UpdateReturnMode.NOTHING);
		return serializedUpdater.getDelta();
	}

	@Override
	public void clear() {
		singleton.set(null);
	}

	@Override
	public U clearAndGetPrevious() {
		var result = singleton.get(null);
		singleton.set(null);
		return this.deserializeValue(result);
	}

	@Override
	public long leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return singleton.get(null) != null ? 1L : 0L;
	}

	@Override
	public boolean isEmpty(@Nullable CompositeSnapshot snapshot) {
		return singleton.get(null) == null;
	}

	@Override
	public Stream<DbProgress<SSTVerificationProgress>> verifyChecksum() {
		return Stream.empty();
	}
}