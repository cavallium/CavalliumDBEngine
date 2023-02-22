package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.jetbrains.annotations.Nullable;

public class ScoringShardsCollectorMultiManager implements CollectorMultiManager<TopDocs, TopDocs> {

	private static final boolean USE_CLASSIC_REDUCE = false;
	private final Query query;
	@Nullable
	private final Sort sort;
	private final int numHits;
	private final FieldDoc after;
	private final int totalHitsThreshold;
	private final @Nullable Integer startN;
	private final @Nullable Integer topN;
	private final CollectorManager<TopFieldCollector, TopFieldDocs> sharedCollectorManager;

	public ScoringShardsCollectorMultiManager(Query query,
			@Nullable final Sort sort,
			final int numHits,
			final FieldDoc after,
			final int totalHitsThreshold,
			int startN,
			int topN) {
		this(query, sort, numHits, after, totalHitsThreshold, (Integer) startN, (Integer) topN);
	}

	public ScoringShardsCollectorMultiManager(Query query,
			@Nullable final Sort sort,
			final int numHits,
			final FieldDoc after,
			final int totalHitsThreshold,
			int startN) {
		this(query, sort, numHits, after, totalHitsThreshold, (Integer) startN, (Integer) 2147483630);
	}

	public ScoringShardsCollectorMultiManager(Query query,
			@Nullable final Sort sort,
			final int numHits,
			final FieldDoc after,
			final int totalHitsThreshold) {
		this(query, sort, numHits, after, totalHitsThreshold, null, null);
	}

	private ScoringShardsCollectorMultiManager(Query query,
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

	public CollectorManager<TopFieldCollector, TopDocs> get(IndexSearcher indexSearcher, int shardIndex) {
		return new CollectorManager<>() {
			@Override
			public TopFieldCollector newCollector() throws IOException {
				return sharedCollectorManager.newCollector();
			}

			@Override
			public TopDocs reduce(Collection<TopFieldCollector> collectors) throws IOException {
				if (LLUtils.isInNonBlockingThread()) {
					throw new UnsupportedOperationException("Called reduce in a nonblocking thread");
				}
				if (USE_CLASSIC_REDUCE) {
					final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
					int i = 0;
					for (TopFieldCollector collector : collectors) {
						topDocs[i++] = collector.topDocs();
					}
					var result = LuceneUtils.mergeTopDocs(sort, null, null, topDocs);

					if (sort != null && sort.needsScores()) {
						TopFieldCollector.populateScores(result.scoreDocs, indexSearcher, query);
					}

					return result;
				} else {
					TopDocs[] topDocs;
					if (sort != null) {
						topDocs = new TopFieldDocs[collectors.size()];
						var i = 0;
						for (TopFieldCollector collector : collectors) {
							topDocs[i] = collector.topDocs();

							// Populate scores of topfieldcollector. By default it doesn't popupate the scores
							if (topDocs[i].scoreDocs.length > 0 && Float.isNaN(topDocs[i].scoreDocs[0].score) && sort.needsScores()) {
								TopFieldCollector.populateScores(topDocs[i].scoreDocs, indexSearcher, query);
							}

							for (ScoreDoc scoreDoc : topDocs[i].scoreDocs) {
								scoreDoc.shardIndex = shardIndex;
							}
							i++;
						}
					} else {
						topDocs = new TopDocs[collectors.size()];
						var i = 0;
						for (TopFieldCollector collector : collectors) {
							topDocs[i] = collector.topDocs();
							for (ScoreDoc scoreDoc : topDocs[i].scoreDocs) {
								scoreDoc.shardIndex = shardIndex;
							}
							i++;
						}
					}
					return LuceneUtils.mergeTopDocs(sort, null, null, topDocs);
				}
			}
		};
	}

	@Override
	public ScoreMode scoreMode() {
		throw new NotImplementedException();
	}

	@SuppressWarnings({"SuspiciousToArrayCall", "IfStatementWithIdenticalBranches"})
	@Override
	public TopDocs reduce(List<TopDocs> topDocs) {
		TopDocs[] arr;
		if (sort != null) {
			arr = topDocs.toArray(TopFieldDocs[]::new);
		} else {
			arr = topDocs.toArray(TopDocs[]::new);
		}
		return LuceneUtils.mergeTopDocs(sort, startN, topN, arr);
	}
}
