package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import reactor.core.publisher.Mono;

public interface LLSearchTransformer {

	LLSearchTransformer NO_TRANSFORMATION = queryParamsMono -> queryParamsMono
			.map(TransformerInput::queryParams);

	record TransformerInput(List<LLIndexSearcher> indexSearchers,
													LocalQueryParams queryParams) {}

	Mono<LocalQueryParams> transform(Mono<TransformerInput> inputMono);
}
