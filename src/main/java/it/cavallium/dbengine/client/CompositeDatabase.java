package it.cavallium.dbengine.client;

import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CompositeDatabase {

	Mono<Void> close();

	/**
	 * Can return SnapshotException
	 */
	Mono<CompositeSnapshot> takeSnapshot();

	/**
	 * Can return SnapshotException
	 */
	Mono<Void> releaseSnapshot(CompositeSnapshot snapshot);

	ByteBufAllocator getAllocator();

	/**
	 * Find corrupted items
	 */
	Flux<BadBlock> badBlocks();

	Mono<Void> verifyChecksum();
}
