package it.cavallium.dbengine.lucene;

import java.util.List;

public record LLFieldDoc(int doc, float score, int shardIndex, List<Object> fields) implements LLDocElement {}
