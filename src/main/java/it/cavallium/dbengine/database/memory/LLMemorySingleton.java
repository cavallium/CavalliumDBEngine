package it.cavallium.dbengine.database.memory;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.disk.BinarySerializationFunction;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.Nullable;

public class LLMemorySingleton implements LLSingleton {

	private final LLMemoryDictionary dict;
	private final String columnNameString;
	private final Buf singletonName;

	public LLMemorySingleton(LLMemoryDictionary dict, String columnNameString, byte[] singletonName) {
		this.dict = dict;
		this.columnNameString = columnNameString;
		this.singletonName = Buf.wrap(singletonName);
	}

	@Override
	public String getDatabaseName() {
		return dict.getDatabaseName();
	}

	@Override
	public Buf get(@Nullable LLSnapshot snapshot) {
		return dict.get(snapshot, singletonName);
	}

	@Override
	public void set(Buf value) {
		dict.put(singletonName, value, LLDictionaryResultType.VOID);
	}

	@Override
	public Buf update(BinarySerializationFunction updater,
			UpdateReturnMode updateReturnMode) {
		return dict.update(singletonName, updater, updateReturnMode);
	}

	@Override
	public LLDelta updateAndGetDelta(BinarySerializationFunction updater) {
		return dict.updateAndGetDelta(singletonName, updater);
	}

	@Override
	public String getColumnName() {
		return columnNameString;
	}

	@Override
	public String getName() {
		return singletonName.toString(StandardCharsets.UTF_8);
	}
}
