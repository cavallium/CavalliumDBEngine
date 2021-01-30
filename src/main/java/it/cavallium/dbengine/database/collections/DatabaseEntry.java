package it.cavallium.dbengine.database.collections;

public interface DatabaseEntry<U> extends DatabaseStage<U> {

	@Override
	default DatabaseEntry<U> entry() {
		return this;
	}
}
