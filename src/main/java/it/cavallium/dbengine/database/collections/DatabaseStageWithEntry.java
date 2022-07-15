package it.cavallium.dbengine.database.collections;

import io.netty5.util.Resource;
import it.cavallium.dbengine.client.BadBlock;
import reactor.core.publisher.Mono;

public interface DatabaseStageWithEntry<T> {

	DatabaseStageEntry<T> entry();
}
