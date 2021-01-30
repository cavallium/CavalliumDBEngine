package it.cavallium.dbengine.database.collections;

public interface DatabaseEntryable<T> {

	DatabaseEntry<T> entry();
}
