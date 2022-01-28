package it.cavallium.dbengine.lucene.mlt;

import com.google.common.collect.Multimap;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import java.io.IOException;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.search.similarities.Similarity;

public class MoreLikeThisTransformer implements GlobalQueryRewrite {

	private final Multimap<String, String> mltDocumentFields;
	private final PerFieldAnalyzerWrapper luceneAnalyzer;
	private final Similarity luceneSimilarity;

	public MoreLikeThisTransformer(Multimap<String, String> mltDocumentFields,
			PerFieldAnalyzerWrapper luceneAnalyzer,
			Similarity luceneSimilarity) {
		this.mltDocumentFields = mltDocumentFields;
		this.luceneAnalyzer = luceneAnalyzer;
		this.luceneSimilarity = luceneSimilarity;
	}

	@Override
	public LocalQueryParams rewrite(LLIndexSearchers indexSearchers, LocalQueryParams queryParams) throws IOException {
		var rewrittenQuery = LuceneUtils.getMoreLikeThisQuery(indexSearchers,
				queryParams,
				luceneAnalyzer,
				luceneSimilarity,
				mltDocumentFields
		);
		return new LocalQueryParams(rewrittenQuery,
				queryParams.offsetLong(),
				queryParams.limitLong(),
				queryParams.pageLimits(),
				queryParams.minCompetitiveScore(),
				queryParams.sort(),
				queryParams.computePreciseHitsCount(),
				queryParams.timeout()
		);
	}
}
