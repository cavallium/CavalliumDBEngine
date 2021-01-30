package it.cavallium.dbengine.database.collections;

public interface DatabaseEntryable<T> {

	DatabaseStageEntry<T> entry();
}
