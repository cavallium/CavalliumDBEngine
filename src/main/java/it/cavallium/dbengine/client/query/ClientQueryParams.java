package it.cavallium.dbengine.client.query;

import io.soabase.recordbuilder.core.RecordBuilder;
import it.cavallium.data.generator.nativedata.Nullablefloat;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.MultiSort;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.QueryParamsBuilder;
import it.cavallium.dbengine.client.query.current.data.ScoreMode;
import it.cavallium.dbengine.database.LLScoreMode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@RecordBuilder
public final record ClientQueryParams<T>(@Nullable CompositeSnapshot snapshot,
																				 @NotNull Query query,
																				 long offset,
																				 long limit,
																				 @Nullable Float minCompetitiveScore,
																				 @Nullable MultiSort<T> sort,
																				 @NotNull LLScoreMode scoreMode) {

	public static <T> ClientQueryParamsBuilder<T> builder() {
		return ClientQueryParamsBuilder
				.<T>builder()
				.snapshot(null)
				.offset(0)
				.limit(Long.MAX_VALUE)
				.minCompetitiveScore(null)
				.sort(null)
				.scoreMode(LLScoreMode.COMPLETE);
	}

	public ScoreMode toScoreMode() {
		return switch (this.scoreMode()) {
			case COMPLETE -> ScoreMode.of(false, true);
			case COMPLETE_NO_SCORES -> ScoreMode.of(false, false);
			case TOP_SCORES -> ScoreMode.of(true, true);
			case NO_SCORES -> ScoreMode.of(true, false);
			//noinspection UnnecessaryDefault
			default -> throw new IllegalArgumentException();
		};
	}

	public QueryParams toQueryParams() {
		return QueryParamsBuilder
				.builder()
				.query(query())
				.sort(sort() != null ? sort().getQuerySort() : NoSort.of())
				.minCompetitiveScore(Nullablefloat.ofNullable(minCompetitiveScore()))
				.offset(offset())
				.limit(limit())
				.scoreMode(toScoreMode())
				.build();
	}
}
