package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.BufDataInput;
import it.cavallium.buffer.BufDataOutput;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.util.concurrent.ForkJoinPool;
import org.jetbrains.annotations.Nullable;

public class DatabaseInt implements LLKeyValueDatabaseStructure {

	private final LLSingleton singleton;
	private final SerializerFixedBinaryLength<Integer> serializer;

	public DatabaseInt(LLSingleton singleton) {
		this.singleton = singleton;
		this.serializer = SerializerFixedBinaryLength.intSerializer();
	}

	public Integer get(@Nullable LLSnapshot snapshot) {
		var result = singleton.get(snapshot);
		return serializer.deserialize(BufDataInput.create(result));
	}

	public void set(int value) {
		var buf = BufDataOutput.createLimited(Integer.BYTES);
		serializer.serialize(value, buf);
		singleton.set(buf.asList());
	}

	@Override
	public String getDatabaseName() {
		return singleton.getDatabaseName();
	}

	@Override
	public ForkJoinPool getDbReadPool() {
		return singleton.getDbReadPool();
	}

	@Override
	public ForkJoinPool getDbWritePool() {
		return singleton.getDbWritePool();
	}
}
