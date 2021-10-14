package it.cavallium.dbengine.lucene.searcher;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer.TransformerInput;
import java.io.Closeable;
import java.io.IOException;
import reactor.core.publisher.Mono;

public class AdaptiveMultiSearcher implements MultiSearcher, Closeable {

	private static final MultiSearcher count
			= new UnsortedUnscoredSimpleMultiSearcher(new CountLocalSearcher());

	private static final MultiSearcher scoredSimple = new ScoredPagedMultiSearcher();

	private static final MultiSearcher unsortedUnscoredPaged
			= new UnsortedUnscoredSimpleMultiSearcher(new PagedLocalSearcher());

	private static final MultiSearcher unsortedUnscoredContinuous
			= new UnsortedUnscoredStreamingMultiSearcher();

	private final UnsortedScoredFullMultiSearcher unsortedScoredFull;

	public AdaptiveMultiSearcher() throws IOException {
		unsortedScoredFull = new UnsortedScoredFullMultiSearcher();
	}

	@Override
	public Mono<Send<LuceneSearchResult>> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		if (transformer == LLSearchTransformer.NO_TRANSFORMATION) {
			return transformedCollectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
		} else {
			return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> transformer
					.transform(Mono.fromCallable(() -> new TransformerInput(indexSearchers, queryParams)))
					.flatMap(queryParams2 -> this
							.transformedCollectMulti(indexSearchersMono, queryParams2, keyFieldName, LLSearchTransformer.NO_TRANSFORMATION)),
					true);
		}
	}

	public Mono<Send<LuceneSearchResult>> transformedCollectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		// offset + limit
		long realLimit = ((long) queryParams.offset() + (long) queryParams.limit());

		return LLUtils.usingSendResource(indexSearchersMono, indexSearchers -> {
			if (queryParams.limit() == 0) {
				return count.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			} else if (queryParams.isSorted() || queryParams.needsScores()) {
				if ((queryParams.isSorted() && !queryParams.isSortedByScore())
						|| realLimit <= (long) queryParams.pageLimits().getPageLimit(0)) {
					return scoredSimple.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
				} else {
					return unsortedScoredFull.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
				}
			} else if (realLimit <= (long) queryParams.pageLimits().getPageLimit(0)) {
				// Run single-page searches using the paged multi searcher
				return unsortedUnscoredPaged.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			} else {
				// Run large/unbounded searches using the continuous multi searcher
				return unsortedUnscoredContinuous.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
			}
		}, true);
	}

	@Override
	public void close() throws IOException {
		unsortedScoredFull.close();
	}

	@Override
	public String getName() {
		return "adaptive local";
	}
}
