package it.cavallium.dbengine.client;

import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

public interface CompositeDatabase {

	Mono<Void> close();

	Mono<CompositeSnapshot> takeSnapshot() throws SnapshotException;

	Mono<Void> releaseSnapshot(CompositeSnapshot snapshot) throws SnapshotException;

	ByteBufAllocator getAllocator();
}
