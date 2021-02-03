package it.cavallium.dbengine.database.collections;

import java.io.IOException;

@SuppressWarnings("SpellCheckingInspection")
public interface JoinerBlocking<KEY, DBVALUE, JOINEDVALUE> {

	interface ValueGetterBlocking<KEY, VALUE> {
		VALUE get(KEY key) throws IOException;
	}

	/**
	 * Warning! You must only join with immutable data to ensure data correctness.
	 * Good examples: message id, send date, ...
	 * Bad examples: message content, views, edited, ...
	 */
	JOINEDVALUE join(ValueGetterBlocking<KEY, DBVALUE> dbValueGetter, DBVALUE value) throws IOException;

	static <KEY, DBVALUE> JoinerBlocking<KEY, DBVALUE, DBVALUE> direct() {
		return (dbValueGetter, value) -> value;
	};
}
