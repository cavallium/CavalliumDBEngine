package it.cavallium.dbengine.client;

import io.soabase.recordbuilder.core.RecordBuilder;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

@RecordBuilder
public record DatabaseOptions(List<DatabaseVolume> volumes, Map<String, String> extraFlags, boolean absoluteConsistency,
															boolean lowMemory, boolean inMemory, boolean useDirectIO, boolean allowMemoryMapping,
															boolean allowNettyDirect, int maxOpenFiles, @Nullable Long memtableMemoryBudgetBytes) {}
