package it.cavallium.dbengine.lucene;

import it.cavallium.dbengine.lucene.searcher.LuceneStreamSearcher.HandleResult;
import java.io.IOException;

public interface LuceneParallelStreamConsumer {

	/**
	 * @param docId document id
	 * @param score score of document
	 */
	HandleResult consume(int docId, float score) throws IOException;
}
