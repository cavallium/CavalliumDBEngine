package it.cavallium.dbengine.lucene.serializer;

public interface SerializedQueryObject {

	/**
	 * returns length|type|---data---
	 */
	void stringify(StringBuilder output);
}
