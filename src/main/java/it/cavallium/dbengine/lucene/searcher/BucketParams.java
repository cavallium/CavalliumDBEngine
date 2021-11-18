package it.cavallium.dbengine.lucene.searcher;

import org.jetbrains.annotations.Nullable;

public record BucketParams(double min, double max, int buckets, String bucketFieldName,
													 @Nullable String valueFieldName) {}
