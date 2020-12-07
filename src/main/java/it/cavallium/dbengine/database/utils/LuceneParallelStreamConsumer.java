package it.cavallium.dbengine.database.utils;

public interface LuceneParallelStreamConsumer {

	/**
	 * @param docId
	 * @return true to continue, false to stop the execution
	 */
	boolean consume(int docId);
}
