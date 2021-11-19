package it.cavallium.dbengine.lucene.collector;

import org.apache.lucene.search.LongValuesSource;

public record LongBucketValueSource(LongValuesSource source) implements BucketValueSource {}
