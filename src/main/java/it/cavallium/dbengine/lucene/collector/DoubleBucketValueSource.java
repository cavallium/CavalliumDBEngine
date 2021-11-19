package it.cavallium.dbengine.lucene.collector;

import org.apache.lucene.search.DoubleValuesSource;

public record DoubleBucketValueSource(DoubleValuesSource source) implements BucketValueSource {}
