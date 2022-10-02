package it.cavallium.dbengine;

import io.netty5.buffer.BufferAllocator;
import it.cavallium.dbengine.DbTestUtils.TempDb;
import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import reactor.core.publisher.Mono;

public interface TemporaryDbGenerator {

	Mono<TempDb> openTempDb(TestAllocator allocator);

	Mono<Void> closeTempDb(TempDb db);
}
