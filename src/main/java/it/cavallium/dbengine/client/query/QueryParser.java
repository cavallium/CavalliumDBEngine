package it.cavallium.dbengine.client.query;

import it.cavallium.dbengine.client.query.current.data.NumericSort;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import org.apache.commons.lang.NotImplementedException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSortField;

public class QueryParser {
	public static Query toQuery(it.cavallium.dbengine.client.query.current.data.Query query) {
		if (query == null) return null;
		throw new NotImplementedException();
	}

	public static boolean isScoringEnabled(QueryParams queryParams) {
		return queryParams.getScoreMode().getComputeScores();
	}

	public static Sort toSort(it.cavallium.dbengine.client.query.current.data.Sort sort) {
		switch (sort.getBasicType$()) {
			case NoSort:
				return null;
			case ScoreSort:
				return new Sort(SortField.FIELD_SCORE);
			case DocSort:
				return new Sort(SortField.FIELD_DOC);
			case NumericSort:
				NumericSort numericSort = (NumericSort) sort;
				return new Sort(new SortedNumericSortField(numericSort.getField(), Type.LONG, numericSort.getReverse()));
			default:
				throw new IllegalStateException("Unexpected value: " + sort.getBasicType$());
		}
	}

	@SuppressWarnings("ConstantConditions")
	public static ScoreMode toScoreMode(it.cavallium.dbengine.client.query.current.data.ScoreMode scoreMode) {
		if (scoreMode.getComputeScores() && scoreMode.getOnlyTopScores()) {
			return ScoreMode.TOP_SCORES;
		} else if (scoreMode.getComputeScores() && !scoreMode.getOnlyTopScores()) {
			return ScoreMode.COMPLETE;
		} else if (!scoreMode.getComputeScores() && scoreMode.getOnlyTopScores()) {
			throw new IllegalStateException("Conflicting score mode options: [computeScores = false, onlyTopScore = true]");
		} else if (!scoreMode.getComputeScores() && !scoreMode.getOnlyTopScores()) {
			return ScoreMode.COMPLETE_NO_SCORES;
		} else {
			throw new IllegalStateException("Unexpected value: " + scoreMode);
		}
	}

	public static it.cavallium.dbengine.client.query.current.data.Term toQueryTerm(Term term) {
		return it.cavallium.dbengine.client.query.current.data.Term.of(term.field(), term.text());
	}
}
