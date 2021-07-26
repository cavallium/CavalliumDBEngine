package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.TIE_BREAKER;
import static it.cavallium.dbengine.lucene.searcher.PaginationInfo.ALLOW_UNSCORED_PAGINATION_MODE;

import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.jetbrains.annotations.Nullable;

public class UnscoredCollectorManager implements
		CollectorManager<TopDocsCollector<ScoreDoc>, TopDocs> {

	private final Supplier<TopDocsCollector<ScoreDoc>> collectorSupplier;
	private final long offset;
	private final long limit;
	private final Sort sort;

	public UnscoredCollectorManager(Supplier<TopDocsCollector<ScoreDoc>> collectorSupplier,
			long offset,
			long limit,
			@Nullable Sort sort) {
		this.collectorSupplier = collectorSupplier;
		this.offset = offset;
		this.limit = limit;
		this.sort = sort;
	}

	@Override
	public TopDocsCollector<ScoreDoc> newCollector() throws IOException {
		return collectorSupplier.get();
	}

	@Override
	public TopDocs reduce(Collection<TopDocsCollector<ScoreDoc>> collection) throws IOException {
		int i = 0;
		TopDocs[] topDocsArray;
		if (sort != null) {
			topDocsArray = new TopFieldDocs[collection.size()];
		} else {
			topDocsArray = new TopDocs[collection.size()];
		}
		for (TopDocsCollector<? extends ScoreDoc> topDocsCollector : collection) {
			var topDocs = topDocsCollector.topDocs();
			for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
				scoreDoc.shardIndex = i;
			}
			topDocsArray[i] = topDocs;
			i++;
		}
		return LuceneUtils.mergeTopDocs(sort,
				LuceneUtils.safeLongToInt(offset),
				LuceneUtils.safeLongToInt(limit),
				topDocsArray,
				TIE_BREAKER
		);
	}
}
