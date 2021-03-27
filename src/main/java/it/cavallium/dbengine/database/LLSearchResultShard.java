package it.cavallium.dbengine.database;

import lombok.Value;
import reactor.core.publisher.Flux;

@Value
public class LLSearchResultShard {

	Flux<LLKeyScore> results;
	long totalHitsCount;
}
