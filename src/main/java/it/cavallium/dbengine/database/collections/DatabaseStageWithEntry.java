package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Resource;
import it.cavallium.dbengine.client.BadBlock;
import reactor.core.publisher.Mono;

public interface DatabaseStageWithEntry<T> {

	DatabaseStageEntry<T> entry();
}
