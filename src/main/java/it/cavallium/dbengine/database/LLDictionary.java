package it.cavallium.dbengine.database;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.VerificationProgress;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public interface LLDictionary extends LLKeyValueDatabaseStructure {

	String getColumnName();

	Buf get(@Nullable LLSnapshot snapshot, Buf key);

	Buf put(Buf key, Buf value, LLDictionaryResultType resultType);

	UpdateMode getUpdateMode();

	default Buf update(Buf key, SerializationFunction<@Nullable Buf, @Nullable Buf> updater, UpdateReturnMode updateReturnMode) {
		LLDelta prev = this.updateAndGetDelta(key, updater);
		return LLUtils.resolveLLDelta(prev, updateReturnMode);
	}

	LLDelta updateAndGetDelta(Buf key, SerializationFunction<@Nullable Buf, @Nullable Buf> updater);

	void clear();

	Buf remove(Buf key, LLDictionaryResultType resultType);

	Stream<OptionalBuf> getMulti(@Nullable LLSnapshot snapshot, Stream<Buf> keys);

	void putMulti(Stream<LLEntry> entries);

	<K> Stream<Boolean> updateMulti(Stream<SerializedKey<K>> keys,
			KVSerializationFunction<K, @Nullable Buf, @Nullable Buf> updateFunction);

	Stream<LLEntry> getRange(@Nullable LLSnapshot snapshot,
			LLRange range,
			boolean reverse,
			boolean smallRange);

	Stream<List<LLEntry>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength,
			boolean smallRange);

	Stream<Buf> getRangeKeys(@Nullable LLSnapshot snapshot,
			LLRange range,
			boolean reverse,
			boolean smallRange);

	Stream<List<Buf>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength,
			boolean smallRange);

	Stream<Buf> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength,
			boolean smallRange);

	Stream<VerificationProgress> verifyChecksum(LLRange range);

	void setRange(LLRange range, Stream<LLEntry> entries, boolean smallRange);

	default void replaceRange(LLRange range,
			boolean canKeysChange,
			Function<@NotNull LLEntry, @NotNull LLEntry> entriesReplacer,
			boolean smallRange) {
		if (canKeysChange) {
			this.setRange(range, this.getRange(null, range, false, smallRange).map(entriesReplacer), smallRange);
		} else {
			this.putMulti(this.getRange(null, range, false, smallRange).map(entriesReplacer));
		}
	}

	boolean isRangeEmpty(@Nullable LLSnapshot snapshot, LLRange range, boolean fillCache);

	long sizeRange(@Nullable LLSnapshot snapshot, LLRange range, boolean fast);

	LLEntry getOne(@Nullable LLSnapshot snapshot, LLRange range);

	Buf getOneKey(@Nullable LLSnapshot snapshot, LLRange range);

	LLEntry removeOne(LLRange range);
}
