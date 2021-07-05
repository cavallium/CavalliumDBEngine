package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;

public class UnsortedCollectorManager implements
		CollectorManager<TopDocsCollector<? extends ScoreDoc>, TopDocs> {

	private final Supplier<TopDocsCollector<? extends ScoreDoc>> collectorSupplier;
	private final long offset;
	private final long limit;

	public UnsortedCollectorManager(Supplier<TopDocsCollector<? extends ScoreDoc>> collectorSupplier, long offset, long limit) {
		this.collectorSupplier = collectorSupplier;
		this.offset = offset;
		this.limit = limit;
	}

	@Override
	public TopDocsCollector<? extends ScoreDoc> newCollector() throws IOException {
		return collectorSupplier.get();
	}

	@Override
	public TopDocs reduce(Collection<TopDocsCollector<? extends ScoreDoc>> collection) throws IOException {
		int i = 0;
		TopDocs[] topDocsArray = new TopDocs[collection.size()];
		for (TopDocsCollector<? extends ScoreDoc> topDocsCollector : collection) {
			var topDocs = topDocsCollector.topDocs();
			for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
				scoreDoc.shardIndex = i;
			}
			topDocsArray[i] = topDocs;
			i++;
		}
		return TopDocs.merge(LuceneUtils.safeLongToInt(offset), LuceneUtils.safeLongToInt(limit), topDocsArray);
	}
}
