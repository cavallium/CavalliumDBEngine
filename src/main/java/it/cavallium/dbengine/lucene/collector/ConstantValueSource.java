package it.cavallium.dbengine.lucene.collector;

public record ConstantValueSource(Number constant) implements BucketValueSource {}
