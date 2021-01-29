package it.cavallium.dbengine.database.utils;

public interface LuceneParallelStreamConsumer {

	/**
	 * @param docId document id
	 * @param score score of document
	 * @return true to continue, false to stop the execution
	 */
	boolean consume(int docId, float score);
}
