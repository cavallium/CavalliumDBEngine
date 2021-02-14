package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.Nullable;

/**
 * Search that only parse query without doing any effective search
 */
public class AllowOnlyQueryParsingCollectorStreamSearcher implements LuceneStreamSearcher {

	public void search(IndexSearcher indexSearcher,
			Query query) throws IOException {
		search(indexSearcher, query, 0, null, null, null, null, null, null);
	}

	@Override
	public void search(IndexSearcher indexSearcher,
			Query query,
			int limit,
			@Nullable Sort luceneSort,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			String keyFieldName,
			Consumer<LLKeyScore> resultsConsumer,
			LongConsumer totalHitsConsumer) throws IOException {
		if (limit > 0) {
			throw new IllegalArgumentException("Limit > 0 not allowed");
		}
		if (luceneSort != null) {
			throw new IllegalArgumentException("Lucene sort not allowed");
		}
		if (scoreMode != null) {
			throw new IllegalArgumentException("Score mode not allowed");
		}
		if (minCompetitiveScore != null) {
			throw new IllegalArgumentException("Minimum competitive score not allowed");
		}
		if (keyFieldName != null) {
			throw new IllegalArgumentException("Key field name not allowed");
		}
		if (resultsConsumer != null) {
			throw new IllegalArgumentException("Results consumer not allowed");
		}
		if (totalHitsConsumer != null) {
			throw new IllegalArgumentException("Total hits consumer not allowed");
		}
		indexSearcher.search(query, new CollectorManager<>() {
			@Override
			public Collector newCollector() {
				return new Collector() {
					@Override
					public LeafCollector getLeafCollector(LeafReaderContext context) {
						return new LeafCollector() {
							@Override
							public void setScorer(Scorable scorer) throws IOException {
								scorer.setMinCompetitiveScore(Float.MAX_VALUE);
							}

							@Override
							public void collect(int doc) {
							}
						};
					}

					@Override
					public ScoreMode scoreMode() {
						return ScoreMode.TOP_SCORES;
					}
				};
			}

			@Override
			public Object reduce(Collection<Collector> collectors) {
				return null;
			}
		});
	}
}
