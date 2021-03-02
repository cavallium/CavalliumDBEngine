package it.cavallium.dbengine.client.query;

import it.cavallium.data.generator.nativedata.Nullablefloat;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.MultiSort;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.ScoreMode;
import it.cavallium.dbengine.database.LLScoreMode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@EqualsAndHashCode
@AllArgsConstructor(
		staticName = "of"
)
@Data
@Builder
@ToString
public final class ClientQueryParams<T> {

	@Nullable
	@Default
	private CompositeSnapshot snapshot = null;

	@NotNull
	@NonNull
	private Query query;

	@Default
	private long limit = Long.MAX_VALUE;

	@Nullable
	@Default
	private Float minCompetitiveScore = null;

	@Nullable
	@Default
	private MultiSort<T> sort = null;

	@NotNull
	@NonNull
	@Default
	private LLScoreMode scoreMode = LLScoreMode.COMPLETE;

	public ScoreMode toScoreMode() {
		ScoreMode scoreMode;
		switch (getScoreMode()) {
			case COMPLETE:
				scoreMode = ScoreMode.of(false, true);
				break;
			case COMPLETE_NO_SCORES:
				scoreMode = ScoreMode.of(false, false);
				break;
			case TOP_SCORES:
				scoreMode = ScoreMode.of(true, true);
				break;
			default:
				throw new IllegalArgumentException();
		}
		return scoreMode;
	}

	public QueryParams toQueryParams() {
		return QueryParams
				.builder()
				.query(getQuery())
				.sort(getSort() != null ? getSort().getQuerySort() : NoSort.of())
				.minCompetitiveScore(Nullablefloat.ofNullable(getMinCompetitiveScore()))
				.limit(getLimit())
				.scoreMode(toScoreMode())
				.build();
	}
}
