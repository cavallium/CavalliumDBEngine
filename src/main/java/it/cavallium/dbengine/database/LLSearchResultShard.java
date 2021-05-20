package it.cavallium.dbengine.database;

import reactor.core.publisher.Flux;

public record LLSearchResultShard (Flux<LLKeyScore> results, long totalHitsCount) {}
