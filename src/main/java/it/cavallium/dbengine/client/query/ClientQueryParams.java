package it.cavallium.dbengine.client.query;

import io.soabase.recordbuilder.core.RecordBuilder;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.Sort;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.QueryParamsBuilder;
import java.time.Duration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@RecordBuilder
public record ClientQueryParams(@Nullable CompositeSnapshot snapshot,
																			@NotNull Query query,
																			long offset,
																			long limit,
																			@Nullable Sort sort,
																			boolean computePreciseHitsCount,
																			@NotNull Duration timeout) {

	public static ClientQueryParamsBuilder builder() {
		return ClientQueryParamsBuilder
				.builder()
				.snapshot(null)
				.offset(0)
				.limit(Long.MAX_VALUE)
				.sort(null)
				// Default timeout: 4 minutes
				.timeout(Duration.ofMinutes(4))
				.computePreciseHitsCount(true);
	}

	public boolean isSorted() {
		return sort != null && sort.isSorted();
	}

	public QueryParams toQueryParams() {
		return QueryParamsBuilder
				.builder()
				.query(query())
				.sort(sort != null ? sort.querySort() : new NoSort())
				.offset(offset())
				.limit(limit())
				.computePreciseHitsCount(computePreciseHitsCount())
				.timeoutMilliseconds(timeout.toMillis())
				.build();
	}
}
