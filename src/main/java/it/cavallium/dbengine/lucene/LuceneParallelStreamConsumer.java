package it.cavallium.dbengine.lucene;

public interface LuceneParallelStreamConsumer {

	/**
	 * @param docId document id
	 * @param score score of document
	 * @return true to continue, false to stop the execution
	 */
	boolean consume(int docId, float score);
}
