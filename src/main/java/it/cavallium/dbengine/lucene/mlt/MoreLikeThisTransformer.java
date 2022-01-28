package it.cavallium.dbengine.lucene.mlt;

import static it.cavallium.dbengine.client.UninterruptibleScheduler.uninterruptibleScheduler;

import com.google.common.collect.Multimap;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import java.io.IOException;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class MoreLikeThisTransformer implements LLSearchTransformer {

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
	public Mono<LocalQueryParams> transform(Mono<TransformerInput> inputMono) {
		return inputMono.publishOn(uninterruptibleScheduler(Schedulers.boundedElastic())).handle((input, sink) -> {
			try {
				var rewrittenQuery = LuceneUtils.getMoreLikeThisQuery(input.indexSearchers(),
						input.queryParams(),
						luceneAnalyzer,
						luceneSimilarity,
						mltDocumentFields
				);
				var queryParams = input.queryParams();
				sink.next(new LocalQueryParams(rewrittenQuery,
						queryParams.offsetLong(),
						queryParams.limitLong(),
						queryParams.pageLimits(),
						queryParams.minCompetitiveScore(),
						queryParams.sort(),
						queryParams.computePreciseHitsCount(),
						queryParams.timeout()
				));
			} catch (IOException ex) {
				sink.error(ex);
			}
		});
	}
}
