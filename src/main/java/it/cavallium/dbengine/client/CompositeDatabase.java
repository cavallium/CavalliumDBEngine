package it.cavallium.dbengine.client;

import reactor.core.publisher.Mono;

public interface CompositeDatabase {

	Mono<Void> close();

	Mono<CompositeSnapshot> takeSnapshot() throws SnapshotException;

	Mono<Void> releaseSnapshot(CompositeSnapshot snapshot) throws SnapshotException;
}
