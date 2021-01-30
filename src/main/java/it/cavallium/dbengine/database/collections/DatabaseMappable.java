package it.cavallium.dbengine.database.collections;

public interface DatabaseMappable<T, U, US extends DatabaseStage<U>> {

	DatabaseStageMap<T, U, US> map();
}
