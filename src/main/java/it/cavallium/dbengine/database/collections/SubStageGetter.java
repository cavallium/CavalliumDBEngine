package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import java.util.Collection;
import java.util.List;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface SubStageGetter<U, US extends DatabaseStage<U>> {

	Mono<US> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<ByteBuf> prefixKey,
			Flux<ByteBuf> debuggingKeyFlux);

	boolean isMultiKey();

	boolean needsDebuggingKeyFlux();
}
