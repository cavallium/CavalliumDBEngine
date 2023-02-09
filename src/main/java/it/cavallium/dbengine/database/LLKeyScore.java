package it.cavallium.dbengine.database;

import org.apache.lucene.index.IndexableField;
import org.jetbrains.annotations.Nullable;

public record LLKeyScore(int docId, int shardId, float score, @Nullable IndexableField key) {}
