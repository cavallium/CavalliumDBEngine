package it.cavallium.dbengine.database.collections;

public class SubStageGetterSingleBytes extends SubStageGetterSingle<byte[]> {

	public SubStageGetterSingleBytes() {
		super(Serializer.noopBytes());
	}
}
