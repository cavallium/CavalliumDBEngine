package it.cavallium.dbengine.lucene.searcher;

import reactor.core.publisher.Mono;

public interface LLSearchTransformer {

	LLSearchTransformer NO_TRANSFORMATION = queryParamsMono -> queryParamsMono;

	Mono<LocalQueryParams> transform(Mono<LocalQueryParams> queryParamsMono);
}
