package it.cavallium.dbengine.client;

import io.soabase.recordbuilder.core.RecordBuilder;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.store.Directory;
import org.jetbrains.annotations.Nullable;

@RecordBuilder
public record LuceneOptions(Map<String, String> extraFlags,
														Duration queryRefreshDebounceTime,
														Duration commitDebounceTime,
														boolean lowMemory,
														LuceneDirectoryOptions directoryOptions,
														long indexWriterBufferSize,
														boolean applyAllDeletes,
														boolean writeAllDeletes,
														boolean allowNonVolatileCollection,
														int maxInMemoryResultEntries) {
}
