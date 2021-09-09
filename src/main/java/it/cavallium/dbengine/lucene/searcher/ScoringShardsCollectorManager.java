package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.TIE_BREAKER;

import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.jetbrains.annotations.Nullable;
import reactor.core.scheduler.Schedulers;

public class ScoringShardsCollectorManager implements CollectorManager<TopFieldCollector, TopDocs> {

	private final Sort sort;
	private final int numHits;
	private final FieldDoc after;
	private final int totalHitsThreshold;
	private final @Nullable Integer startN;
	private final @Nullable Integer topN;
	private final CollectorManager<TopFieldCollector, TopFieldDocs> sharedCollectorManager;

	public ScoringShardsCollectorManager(final Sort sort,
			final int numHits,
			final FieldDoc after,
			final int totalHitsThreshold,
			int startN,
			int topN) {
		this(sort, numHits, after, totalHitsThreshold, (Integer) startN, (Integer) topN);
	}

	public ScoringShardsCollectorManager(final Sort sort,
			final int numHits,
			final FieldDoc after,
			final int totalHitsThreshold) {
		this(sort, numHits, after, totalHitsThreshold, null, null);
	}

	private ScoringShardsCollectorManager(final Sort sort,
			final int numHits,
			final FieldDoc after,
			final int totalHitsThreshold,
			@Nullable Integer startN,
			@Nullable Integer topN) {
		this.sort = sort;
		this.numHits = numHits;
		this.after = after;
		this.totalHitsThreshold = totalHitsThreshold;
		this.startN = startN;
		this.topN = topN;
		this.sharedCollectorManager = TopFieldCollector.createSharedManager(sort, numHits, after, totalHitsThreshold);
	}

	@Override
	public TopFieldCollector newCollector() throws IOException {
		return sharedCollectorManager.newCollector();
	}

	@Override
	public TopDocs reduce(Collection<TopFieldCollector> collectors) throws IOException {
		if (Schedulers.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called reduce in a nonblocking thread");
		}
		TopDocs result;
		if (sort != null) {
			TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
			var i = 0;
			for (TopFieldCollector collector : collectors) {
				topDocs[i] = collector.topDocs();
				for (ScoreDoc scoreDoc : topDocs[i].scoreDocs) {
					scoreDoc.shardIndex = i;
				}
				i++;
			}
			result = LuceneUtils.mergeTopDocs(sort, startN, topN, topDocs, TIE_BREAKER);
		} else {
			TopDocs[] topDocs = new TopDocs[collectors.size()];
			var i = 0;
			for (TopFieldCollector collector : collectors) {
				topDocs[i] = collector.topDocs();
				for (ScoreDoc scoreDoc : topDocs[i].scoreDocs) {
					scoreDoc.shardIndex = i;
				}
				i++;
			}
			result = LuceneUtils.mergeTopDocs(null, startN, topN, topDocs, TIE_BREAKER);
		}
		return result;
	}
}
