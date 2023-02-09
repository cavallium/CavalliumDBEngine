package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.buffers.BufDataInput;
import it.cavallium.dbengine.buffers.BufDataOutput;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.disk.BinarySerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

public final class DatabaseMapSingle<U> implements DatabaseStageEntry<U> {

	private static final Logger LOG = LogManager.getLogger(DatabaseMapSingle.class);

	private final LLDictionary dictionary;
	private final Buf key;
	private final Serializer<U> serializer;

	public DatabaseMapSingle(LLDictionary dictionary, Buf key, Serializer<U> serializer) {
		this.dictionary = dictionary;
		this.key = key;
		this.serializer = serializer;
	}

	private LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	private U deserializeValue(Buf value) {
		try {
			return serializer.deserialize(BufDataInput.create(value));
		} catch (IndexOutOfBoundsException ex) {
			var exMessage = ex.getMessage();
			if (exMessage != null && exMessage.contains("read 0 to 0, write 0 to ")) {
				LOG.error("Unexpected zero-bytes value at %s:%s:%s".formatted(dictionary.getDatabaseName(),
						dictionary.getColumnName(),
						LLUtils.toStringSafe(key)
				));
				return null;
			} else {
				throw ex;
			}
		}
	}

	private Buf serializeValue(U value) throws SerializationException {
		BufDataOutput valBuf = BufDataOutput.create(serializer.getSerializedSizeHint());
		serializer.serialize(value, valBuf);
		return valBuf.asList();
	}

	@Override
	public U get(@Nullable CompositeSnapshot snapshot) {
		var result = dictionary.get(resolveSnapshot(snapshot), key);
		if (result != null) {
			return deserializeValue(result);
		} else {
			return null;
		}
	}

	@Override
	public U setAndGetPrevious(U value) {
		var serializedKey = value != null ? serializeValue(value) : null;
		var result = dictionary.put(key, serializedKey, LLDictionaryResultType.PREVIOUS_VALUE);
		if (result != null) {
			return deserializeValue(result);
		} else {
			return null;
		}
	}

	@Override
	public U update(SerializationFunction<@Nullable U, @Nullable U> updater,
			UpdateReturnMode updateReturnMode) {
		Buf resultBytes = dictionary.update(key, this.createUpdater(updater), updateReturnMode);
		return deserializeValue(resultBytes);
	}

	@Override
	public Delta<U> updateAndGetDelta(SerializationFunction<@Nullable U, @Nullable U> updater) {
		var delta = dictionary.updateAndGetDelta(key, this.createUpdater(updater));
		return LLUtils.mapLLDelta(delta, bytes -> serializer.deserialize(BufDataInput.create(bytes)));
	}

	private BinarySerializationFunction createUpdater(SerializationFunction<U, U> updater) {
		return oldBytes -> {
			U result;
			if (oldBytes == null) {
				result = updater.apply(null);
			} else {
				U deserializedValue = serializer.deserialize(BufDataInput.create(oldBytes));
				result = updater.apply(deserializedValue);
			}
			if (result == null) {
				return null;
			} else {
				return serializeValue(result);
			}
		};
	}

	@Override
	public U clearAndGetPrevious() {
		return deserializeValue(dictionary.remove(key, LLDictionaryResultType.PREVIOUS_VALUE));
	}

	@Override
	public long leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(key), false) ? 0L : 1L;
	}

	@Override
	public boolean isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(key), true);
	}

	@Override
	public Stream<BadBlock> badBlocks() {
		return dictionary.badBlocks(LLRange.single(key));
	}

}