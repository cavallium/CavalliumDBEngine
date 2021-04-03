package it.cavallium.dbengine.database.collections;

import java.io.IOException;

public interface JoinerBlocking<KEY, DB_VALUE, JOINED_VALUE> {

	interface ValueGetterBlocking<KEY, VALUE> {
		VALUE get(KEY key) throws IOException;
	}

	/**
	 * Warning! You must only join with immutable data to ensure data correctness.
	 * Good examples: message id, send date, ...
	 * Bad examples: message content, views, edited, ...
	 */
	JOINED_VALUE join(ValueGetterBlocking<KEY, DB_VALUE> dbValueGetter, DB_VALUE value) throws IOException;

}
