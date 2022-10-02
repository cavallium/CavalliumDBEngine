package it.cavallium.dbengine.database.collections;

import io.netty5.buffer.Buffer;
import io.netty5.util.Send;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public interface SubStageGetter<U, US extends DatabaseStage<U>> {

	Mono<US> subStage(LLDictionary dictionary,
			@Nullable CompositeSnapshot snapshot,
			Mono<Buffer> prefixKey);

}
