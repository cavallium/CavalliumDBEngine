package it.cavallium.dbengine.lucene.searcher;

import static it.cavallium.dbengine.lucene.LuceneUtils.safeLongToInt;

import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.PageLimits;
import java.util.Objects;
import java.util.Optional;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record LocalQueryParams(@NotNull Query query, int offsetInt, long offsetLong, int limitInt, long limitLong,
															 @NotNull PageLimits pageLimits,
															 @Nullable Float minCompetitiveScore, @Nullable Sort sort, boolean complete) {

	public LocalQueryParams(@NotNull Query query,
			long offsetLong,
			long limitLong,
			@NotNull PageLimits pageLimits,
			@Nullable Float minCompetitiveScore,
			@Nullable Sort sort,
			boolean complete) {
		this(query, safeLongToInt(offsetLong), offsetLong, safeLongToInt(limitLong), limitLong, pageLimits,
				minCompetitiveScore, sort, complete);
	}

	public LocalQueryParams(@NotNull Query query,
			int offsetInt,
			int limitInt,
			@NotNull PageLimits pageLimits,
			@Nullable Float minCompetitiveScore,
			@Nullable Sort sort,
			boolean complete) {
		this(query, offsetInt, offsetInt, limitInt, limitInt, pageLimits, minCompetitiveScore, sort, complete);
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

	public Optional<Boolean> needsScoresOptional() {
		if (sort == null) return Optional.empty();
		return Optional.of(sort.needsScores());
	}

	public ScoreMode getScoreMode() {
		if (complete) {
			return needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
		} else {
			return needsScores() ? ScoreMode.TOP_DOCS_WITH_SCORES : ScoreMode.TOP_DOCS;
		}
	}

	public Optional<ScoreMode> getScoreModeOptional() {
		if (complete) {
			return needsScoresOptional().map(needsScores -> needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES);
		} else {
			return needsScoresOptional().map(needsScores -> needsScores ? ScoreMode.TOP_DOCS_WITH_SCORES : ScoreMode.TOP_DOCS);
		}
	}

	public int getTotalHitsThresholdInt() {
		return LuceneUtils.totalHitsThreshold(this.complete);
	}

	public long getTotalHitsThresholdLong() {
		return LuceneUtils.totalHitsThresholdLong(this.complete);
	}
}
