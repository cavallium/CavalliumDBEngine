package it.cavallium.dbengine.database;

import reactor.core.publisher.Mono;

public interface LLSnapshottable {

	Mono<LLSnapshot> takeSnapshot();

	Mono<Void> releaseSnapshot(LLSnapshot snapshot);
}
