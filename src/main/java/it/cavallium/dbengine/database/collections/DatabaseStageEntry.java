package it.cavallium.dbengine.database.collections;

import io.net5.buffer.api.Resource;
import it.cavallium.dbengine.client.BadBlock;
import reactor.core.publisher.Flux;

public interface DatabaseStageEntry<U> extends DatabaseStage<U> {

	@Override
	default DatabaseStageEntry<U> entry() {
		return this;
	}
}
