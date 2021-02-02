package it.cavallium.dbengine.database.collections;

import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;

public class QueryableBuilder {

	public QueryableBuilder(int stagesNumber) {

	}

	public SerializerFixedBinaryLength<byte[], byte[]> serializer() {
		return null;
	}

	public <T, U extends SubStageGetterSingle<T>> SubStageGetterSingleBytes tail(U ssg,
			SerializerFixedBinaryLength<T, byte[]> ser) {
		return null;

	}

	public <T, U, US extends DatabaseStage<U>, M extends DatabaseStageMap<T, U, US>> M wrap(M map) {
		return null;
	}
}
