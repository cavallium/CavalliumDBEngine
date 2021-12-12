package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.LuceneUtils.safeLongToInt;

import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.PageLimits;
import java.time.Duration;
import java.util.Objects;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record LocalQueryParams(@NotNull Query query, int offsetInt, long offsetLong, int limitInt, long limitLong,
															 @NotNull PageLimits pageLimits, @Nullable Float minCompetitiveScore, @Nullable Sort sort,
															 boolean computePreciseHitsCount, Duration timeout) {

	public LocalQueryParams(@NotNull Query query,
			long offsetLong,
			long limitLong,
			@NotNull PageLimits pageLimits,
			@Nullable Float minCompetitiveScore,
			@Nullable Sort sort,
			boolean computePreciseHitsCount,
			Duration timeout) {
		this(query, safeLongToInt(offsetLong), offsetLong, safeLongToInt(limitLong), limitLong, pageLimits,
				minCompetitiveScore, sort, computePreciseHitsCount, timeout);
	}

	public LocalQueryParams(@NotNull Query query,
			int offsetInt,
			int limitInt,
			@NotNull PageLimits pageLimits,
			@Nullable Float minCompetitiveScore,
			@Nullable Sort sort,
			boolean computePreciseHitsCount,
			Duration timeout) {
		this(query,
				offsetInt,
				offsetInt,
				limitInt,
				limitInt,
				pageLimits,
				minCompetitiveScore,
				sort,
				computePreciseHitsCount,
				timeout
		);
	}

	public boolean isSorted() {
		return sort != null;
	}

	public boolean isSortedByScore() {
		return Objects.equals(sort, Sort.RELEVANCE);
	}

	public boolean needsScores() {
		return sort != null && sort.needsScores();
	}

	public int getTotalHitsThresholdInt() {
		return LuceneUtils.totalHitsThreshold(this.computePreciseHitsCount);
	}

	public long getTotalHitsThresholdLong() {
		return LuceneUtils.totalHitsThresholdLong(this.computePreciseHitsCount);
	}
}
