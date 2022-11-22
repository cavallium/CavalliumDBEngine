package it.cavallium.dbengine.database;

import it.cavallium.dbengine.rpc.current.data.Column;
import java.nio.file.Path;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public interface DatabaseOperations {

	Mono<Void> ingestSST(Column column, Publisher<Path> files, boolean replaceExisting);
}
