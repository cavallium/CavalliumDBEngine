package it.cavallium.dbengine.database.disk;

import io.soabase.recordbuilder.core.RecordBuilder;
import it.cavallium.dbengine.database.Column;
import java.util.List;
import java.util.Map;

@RecordBuilder
public record DatabaseOptions(Map<String, String> extraFlags,
															boolean absoluteConsistency,
															boolean lowMemory,
															boolean inMemory,
															boolean useDirectIO,
															boolean allowMemoryMapping) {}
