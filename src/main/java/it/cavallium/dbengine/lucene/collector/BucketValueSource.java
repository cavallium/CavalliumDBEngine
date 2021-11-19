package it.cavallium.dbengine.lucene.collector;

public sealed interface BucketValueSource permits DoubleBucketValueSource, LongBucketValueSource, ConstantValueSource,
		NullValueSource {}
