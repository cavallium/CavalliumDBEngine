package it.cavallium.dbengine.client.query;

import io.soabase.recordbuilder.core.RecordBuilder;
import it.cavallium.data.generator.nativedata.Nullablefloat;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.MultiSort;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.QueryParamsBuilder;
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
																				 boolean complete) {

	public static <T> ClientQueryParamsBuilder<T> builder() {
		return ClientQueryParamsBuilder
				.<T>builder()
				.snapshot(null)
				.offset(0)
				.limit(Long.MAX_VALUE)
				.minCompetitiveScore(null)
				.sort(null)
				.complete(true);
	}

	public boolean isSorted() {
		return sort != null && sort.isSorted();
	}

	public QueryParams toQueryParams() {
		return QueryParamsBuilder
				.builder()
				.query(query())
				.sort(sort != null ? sort.getQuerySort() : new NoSort())
				.minCompetitiveScore(Nullablefloat.ofNullable(minCompetitiveScore()))
				.offset(offset())
				.limit(limit())
				.complete(complete())
				.build();
	}
}
