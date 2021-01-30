package it.cavallium.dbengine.database.collections;

public interface DatabaseStageEntry<U> extends DatabaseStage<U> {

	@Override
	default DatabaseStageEntry<U> entry() {
		return this;
	}
}
