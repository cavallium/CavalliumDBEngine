package it.cavallium.luceneserializer.luceneserializer;

public interface SerializedQueryObject {

	/**
	 * @return length|type|---data---
	 */
	void stringify(StringBuilder output);
}
