package it.cavallium.dbengine.lucene.searcher;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record LocalQueryParams(@NotNull Query query, int offset, int limit,
															 @Nullable Float minCompetitiveScore, @Nullable Sort sort,
															 @NotNull ScoreMode scoreMode) {}
