package it.cavallium.dbengine.lucene.collector;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;

public interface CollectorMultiManager<T, U> {

	ScoreMode scoreMode();

	U reduce(List<T> results) throws IOException;
}
