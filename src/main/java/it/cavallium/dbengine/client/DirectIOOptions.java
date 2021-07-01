package it.cavallium.dbengine.client;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record DirectIOOptions(boolean alwaysForceDirectIO, int mergeBufferSize, long minBytesDirect) {}
