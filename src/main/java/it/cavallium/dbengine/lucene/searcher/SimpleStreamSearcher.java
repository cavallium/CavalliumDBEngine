package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.LuceneUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;

/**
 * Sorted search (slower and more memory-intensive)
 */
public class SimpleStreamSearcher implements LuceneStreamSearcher {

	@Override
	public LuceneSearchInstance search(IndexSearcher indexSearcher,
			Query query,
			int offset,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName) throws IOException {
		var searcher = new TopDocsSearcher(indexSearcher,
				query,
				luceneSort,
				offset + limit,
				null,
				scoreMode != ScoreMode.COMPLETE_NO_SCORES,
				1000
		);
		return new LuceneSearchInstance() {
			@Override
			public long getTotalHitsCount() throws IOException {
				return searcher.getTopDocs(0, 1).totalHits.value;
			}

			@Override
			public void getResults(ResultItemConsumer resultsConsumer) throws IOException {
				ObjectArrayList<ScoreDoc> hits = ObjectArrayList.wrap(searcher.getTopDocs(offset, limit).scoreDocs);
				for (ScoreDoc hit : hits) {
					int docId = hit.doc;
					float score = hit.score;
					if (LuceneUtils.collectTopDoc(logger,
							docId,
							score,
							minCompetitiveScore,
							indexSearcher,
							keyFieldName,
							resultsConsumer
					) == HandleResult.HALT) {
						return;
					}
				}
			}
		};
	}
}
