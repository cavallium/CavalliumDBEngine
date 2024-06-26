package it.cavallium.dbengine.database.memory;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationFunction;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ForkJoinPool;
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
	public ForkJoinPool getDbReadPool() {
		return dict.getDbReadPool();
	}

	@Override
	public ForkJoinPool getDbWritePool() {
		return dict.getDbWritePool();
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
	public Buf update(SerializationFunction<@Nullable Buf, @Nullable Buf> updater,
			UpdateReturnMode updateReturnMode) {
		return dict.update(singletonName, updater, updateReturnMode);
	}

	@Override
	public LLDelta updateAndGetDelta(SerializationFunction<@Nullable Buf, @Nullable Buf> updater) {
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
