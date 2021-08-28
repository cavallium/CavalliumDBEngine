package it.cavallium.dbengine.client;

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
															boolean allowMemoryMapping,
															boolean allowNettyDirect,
															boolean useNettyDirect,
															int maxOpenFiles) {}
