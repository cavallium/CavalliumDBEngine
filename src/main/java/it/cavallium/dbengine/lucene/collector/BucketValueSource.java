package it.cavallium.dbengine.lucene.collector;

import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.LongValuesSource;

public sealed interface BucketValueSource permits BucketValueSource.DoubleBucketValueSource,
		BucketValueSource.LongBucketValueSource,
		BucketValueSource.ConstantValueSource, BucketValueSource.NullValueSource {

	record ConstantValueSource(Number constant) implements BucketValueSource {}

	record DoubleBucketValueSource(DoubleValuesSource source) implements BucketValueSource {}

	record LongBucketValueSource(LongValuesSource source) implements BucketValueSource {}

	record NullValueSource() implements BucketValueSource {}
}
