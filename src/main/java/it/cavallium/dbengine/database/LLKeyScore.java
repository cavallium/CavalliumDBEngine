package it.cavallium.dbengine.database;

import java.util.Objects;
import java.util.StringJoiner;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public record LLKeyScore(int docId, float score, @Nullable BytesRef key) {}
