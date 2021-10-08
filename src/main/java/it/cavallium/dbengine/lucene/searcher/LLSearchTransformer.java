package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import reactor.core.publisher.Mono;

public interface LLSearchTransformer {

	LLSearchTransformer NO_TRANSFORMATION = queryParamsMono -> queryParamsMono
			.map(TransformerInput::queryParams);

	record TransformerInput(LLIndexSearchers indexSearchers,
													LocalQueryParams queryParams) {}

	Mono<LocalQueryParams> transform(Mono<TransformerInput> inputMono);
}
