package it.cavallium.dbengine.database;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public record LLSearchResultShard (Flux<LLKeyScore> results, long totalHitsCount, Mono<Void> release) {}
