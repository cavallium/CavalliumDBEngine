package it.cavallium.dbengine.client;

import io.micrometer.core.instrument.MeterRegistry;
import io.net5.buffer.api.BufferAllocator;
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

	BufferAllocator getAllocator();

	MeterRegistry getMeterRegistry();

	/**
	 * Find corrupted items
	 */
	Flux<BadBlock> badBlocks();

	Mono<Void> verifyChecksum();
}
