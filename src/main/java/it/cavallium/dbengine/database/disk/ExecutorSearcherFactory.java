package it.cavallium.dbengine.database.disk;

import java.util.concurrent.Executor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;

public class ExecutorSearcherFactory extends SearcherFactory {

	private final Executor executor;

	public ExecutorSearcherFactory(Executor executor) {
		this.executor = executor;
	}

	@Override
	public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) {
		return new IndexSearcher(reader, executor);
	}
}
