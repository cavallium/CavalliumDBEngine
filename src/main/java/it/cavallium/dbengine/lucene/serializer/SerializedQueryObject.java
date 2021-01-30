package it.cavallium.dbengine.lucene.serializer;

public interface SerializedQueryObject {

	/**
	 * @return length|type|---data---
	 */
	void stringify(StringBuilder output);
}
