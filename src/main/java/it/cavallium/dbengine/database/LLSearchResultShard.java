package it.cavallium.dbengine.database;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public record LLSearchResultShard (Flux<LLKeyScore> results, TotalHitsCount totalHitsCount, Mono<Void> release) {}
