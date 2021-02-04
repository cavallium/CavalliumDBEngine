package it.cavallium.dbengine.lucene;

import java.io.IOException;

public interface LuceneParallelStreamConsumer {

	/**
	 * @param docId document id
	 * @param score score of document
	 * @return true to continue, false to stop the execution
	 */
	boolean consume(int docId, float score) throws IOException;
}
