package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.searcher.LuceneStreamSearcher.ResultItemConsumer;
import java.io.IOException;

public interface LuceneSearchInstance {

	long getTotalHitsCount() throws IOException;

	void getResults(ResultItemConsumer consumer) throws IOException;
}
