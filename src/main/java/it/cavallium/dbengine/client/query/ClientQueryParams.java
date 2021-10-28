package it.cavallium.dbengine.client.query;

import io.soabase.recordbuilder.core.RecordBuilder;
import it.cavallium.data.generator.nativedata.Nullablefloat;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.Sort;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.QueryParamsBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@RecordBuilder
public final record ClientQueryParams(@Nullable CompositeSnapshot snapshot,
																			@NotNull Query query,
																			long offset,
																			long limit,
																			@Nullable Float minCompetitiveScore,
																			@Nullable Sort sort,
																			boolean complete) {

	public static ClientQueryParamsBuilder builder() {
		return ClientQueryParamsBuilder
				.builder()
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
				.sort(sort != null ? sort.querySort() : new NoSort())
				.minCompetitiveScore(Nullablefloat.ofNullable(minCompetitiveScore()))
				.offset(offset())
				.limit(limit())
				.complete(complete())
				.build();
	}
}
