package it.cavallium.dbengine.client;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record NRTCachingOptions(double maxMergeSizeMB, double maxCachedMB) {}
