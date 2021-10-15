package it.cavallium.dbengine.lucene.collector;

import static it.cavallium.dbengine.lucene.searcher.CurrentPageInfo.TIE_BREAKER;

import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.jetbrains.annotations.Nullable;
import reactor.core.scheduler.Schedulers;

public class ScoringShardsCollectorManager implements CollectorManager<TopFieldCollector, TopDocs> {

	private final Query query;
	@Nullable
	private final Sort sort;
	private final int numHits;
	private final FieldDoc after;
	private final int totalHitsThreshold;
	private final @Nullable Integer startN;
	private final @Nullable Integer topN;
	private final CollectorManager<TopFieldCollector, TopFieldDocs> sharedCollectorManager;
	private List<IndexSearcher> indexSearchers;

	public ScoringShardsCollectorManager(Query query,
			@Nullable final Sort sort,
			final int numHits,
			final FieldDoc after,
			final int totalHitsThreshold,
			int startN,
			int topN) {
		this(query, sort, numHits, after, totalHitsThreshold, (Integer) startN, (Integer) topN);
	}

	public ScoringShardsCollectorManager(Query query,
			@Nullable final Sort sort,
			final int numHits,
			final FieldDoc after,
			final int totalHitsThreshold,
			int startN) {
		this(query, sort, numHits, after, totalHitsThreshold, (Integer) startN, (Integer) 2147483630);
	}

	public ScoringShardsCollectorManager(Query query,
			@Nullable final Sort sort,
			final int numHits,
			final FieldDoc after,
			final int totalHitsThreshold) {
		this(query, sort, numHits, after, totalHitsThreshold, null, null);
	}

	private ScoringShardsCollectorManager(Query query,
			@Nullable final Sort sort,
			final int numHits,
			final FieldDoc after,
			final int totalHitsThreshold,
			@Nullable Integer startN,
			@Nullable Integer topN) {
		this.query = query;
		this.sort = sort;
		this.numHits = numHits;
		this.after = after;
		this.totalHitsThreshold = totalHitsThreshold;
		this.startN = startN;
		if (topN != null && startN != null && (long) topN + (long) startN > 2147483630) {
			this.topN	= 2147483630 - startN;
		} else if (topN != null && topN > 2147483630) {
			this.topN = 2147483630;
		} else {
			this.topN = topN;
		}
		this.sharedCollectorManager = TopFieldCollector.createSharedManager(sort == null ? Sort.RELEVANCE : sort, numHits, after, totalHitsThreshold);
	}

	@Override
	public TopFieldCollector newCollector() throws IOException {
		return sharedCollectorManager.newCollector();
	}

	public void setIndexSearchers(List<IndexSearcher> indexSearcher) {
		this.indexSearchers = indexSearcher;
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

				// Populate scores of topfieldcollector. By default it doesn't popupate the scores
				if (topDocs[i].scoreDocs.length > 0 && Float.isNaN(topDocs[i].scoreDocs[0].score) && sort.needsScores()) {
					Objects.requireNonNull(indexSearchers, "You must call setIndexSearchers before calling reduce!");
					TopFieldCollector.populateScores(topDocs[i].scoreDocs, indexSearchers.get(i), query);
				}

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
