package it.cavallium.dbengine.database;

import it.cavallium.dbengine.rpc.current.data.Column;
import java.nio.file.Path;
import java.util.stream.Stream;

public interface DatabaseOperations {

	void ingestSST(Column column, Stream<Path> files, boolean replaceExisting);
}
