package it.cavallium.dbengine.database.collections;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.BufDataInput;
import it.cavallium.buffer.BufDataOutput;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import org.jetbrains.annotations.Nullable;

public class DatabaseLong implements LLKeyValueDatabaseStructure {

	private final LLSingleton singleton;
	private final SerializerFixedBinaryLength<Long> serializer;
	private final SerializerFixedBinaryLength<Integer> bugSerializer;

	public DatabaseLong(LLSingleton singleton) {
		this.singleton = singleton;
		this.serializer = SerializerFixedBinaryLength.longSerializer();
		this.bugSerializer = SerializerFixedBinaryLength.intSerializer();
	}

	public Long get(@Nullable LLSnapshot snapshot) {
		var result = BufDataInput.create(singleton.get(snapshot));
		if (result.available() == 4) {
			return (long) (int) bugSerializer.deserialize(result);
		} else {
			return serializer.deserialize(result);
		}
	}

	public Long incrementAndGet() {
		return addAnd(1, UpdateReturnMode.GET_NEW_VALUE);
	}

	public Long getAndIncrement() {
		return addAnd(1, UpdateReturnMode.GET_OLD_VALUE);
	}

	public Long decrementAndGet() {
		return addAnd(-1, UpdateReturnMode.GET_NEW_VALUE);
	}

	public Long getAndDecrement() {
		return addAnd(-1, UpdateReturnMode.GET_OLD_VALUE);
	}

	public Long addAndGet(long count) {
		return addAnd(count, UpdateReturnMode.GET_NEW_VALUE);
	}

	public Long getAndAdd(long count) {
		return addAnd(count, UpdateReturnMode.GET_OLD_VALUE);
	}

	private Long addAnd(long count, UpdateReturnMode updateReturnMode) {
		var result = singleton.update(prev -> {
			if (prev != null) {
				var prevLong = prev.getLong(0);
				var buf = Buf.createZeroes(Long.BYTES);
				buf.setLong(0, prevLong + count);
				return buf;
			} else {
				var buf = Buf.createZeroes(Long.BYTES);
				buf.setLong(0, count);
				return buf;
			}
		}, updateReturnMode);
		return result.getLong(0);
	}

	public void set(long value) {
		var buf = BufDataOutput.createLimited(Long.BYTES);
		serializer.serialize(value, buf);
		singleton.set(buf.asList());
	}

	@Override
	public String getDatabaseName() {
		return singleton.getDatabaseName();
	}
}
