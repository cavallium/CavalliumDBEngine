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
		var firstTopDocs = searcher.getTopDocs(0, 1);
		long totalHitsCount = firstTopDocs.totalHits.value;
		return new LuceneSearchInstance() {
			@Override
			public long getTotalHitsCount() {
				return totalHitsCount;
			}

			@Override
			public void getResults(ResultItemConsumer resultsConsumer) throws IOException {
				if (firstTopDocs.scoreDocs.length > 0) {
					{
						var hit = firstTopDocs.scoreDocs[0];
						if (publishHit(hit, resultsConsumer) == HandleResult.HALT) {
							return;
						}
					}
					ObjectArrayList<ScoreDoc> hits = ObjectArrayList.wrap(searcher.getTopDocs(offset, limit - 1).scoreDocs);
					for (ScoreDoc hit : hits) {
						if (publishHit(hit, resultsConsumer) == HandleResult.HALT) {
							return;
						}
					}
				}
			}

			private HandleResult publishHit(ScoreDoc hit, ResultItemConsumer resultsConsumer) throws IOException {
				int docId = hit.doc;
				float score = hit.score;
				return LuceneUtils.collectTopDoc(logger,
						docId,
						score,
						minCompetitiveScore,
						indexSearcher,
						keyFieldName,
						resultsConsumer
				);
			}
		};
	}
}
