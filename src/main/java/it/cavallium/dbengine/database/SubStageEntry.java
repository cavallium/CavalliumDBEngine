package it.cavallium.dbengine.database;

import it.cavallium.dbengine.database.collections.DatabaseStage;
import java.util.Map.Entry;
import java.util.Objects;

public final class SubStageEntry<T, U extends DatabaseStage<?>> implements DiscardingCloseable, Entry<T, U> {

	private final T key;
	private final U value;

	public SubStageEntry(T key, U value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public void close() {
		if (value != null) {
			value.close();
		}
	}

	@Override
	public T getKey() {
		return key;
	}

	@Override
	public U getValue() {
		return value;
	}

	@Override
	public U setValue(U value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;
		//noinspection rawtypes
		var that = (SubStageEntry) obj;
		return Objects.equals(this.key, that.key) && Objects.equals(this.value, that.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, value);
	}

	@Override
	public String toString() {
		return "SubStageEntry[" + "key=" + key + ", " + "value=" + value + ']';
	}

}
