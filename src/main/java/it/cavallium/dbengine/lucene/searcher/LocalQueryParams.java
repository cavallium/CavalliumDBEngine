package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.PageLimits;
import java.util.Optional;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record LocalQueryParams(@NotNull Query query, int offset, int limit, @NotNull PageLimits pageLimits,
															 @Nullable Float minCompetitiveScore, @Nullable Sort sort, boolean complete) {

	public boolean isSorted() {
		return sort != null;
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

	public int getTotalHitsThreshold() {
		return LuceneUtils.totalHitsThreshold(this.complete);
	}
}
