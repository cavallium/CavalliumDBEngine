package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;
import java.nio.file.Path;

public record RocksDBFileMetadata(Path filePath, String fileName, int level, String columnName, long numEntries, long size,
																	LLRange keysRange) {
	
}
