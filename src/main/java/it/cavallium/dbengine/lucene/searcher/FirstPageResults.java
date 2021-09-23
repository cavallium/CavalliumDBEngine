package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import reactor.core.publisher.Flux;

record FirstPageResults(TotalHitsCount totalHitsCount, Flux<LLKeyScore> firstPageHitsFlux,
												CurrentPageInfo nextPageInfo) {}
