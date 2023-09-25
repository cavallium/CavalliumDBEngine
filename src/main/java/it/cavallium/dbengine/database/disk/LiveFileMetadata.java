package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLRange;

public record LiveFileMetadata(String path, String fileName, int level, String columnName, long numEntries, long size,
															 LLRange keysRange) {
	
}
