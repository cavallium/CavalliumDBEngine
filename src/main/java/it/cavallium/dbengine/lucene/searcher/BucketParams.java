package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.collector.BucketValueSource;
import org.jetbrains.annotations.NotNull;

public record BucketParams(double min, double max, int buckets, String bucketFieldName,
													 @NotNull BucketValueSource valueSource) {}
