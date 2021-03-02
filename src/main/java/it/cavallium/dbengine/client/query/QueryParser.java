package it.cavallium.dbengine.client.query;

import it.cavallium.dbengine.client.query.current.data.BooleanQueryPart;
import it.cavallium.dbengine.client.query.current.data.BoostQuery;
import it.cavallium.dbengine.client.query.current.data.BoxedQuery;
import it.cavallium.dbengine.client.query.current.data.ConstantScoreQuery;
import it.cavallium.dbengine.client.query.current.data.IntPointExactQuery;
import it.cavallium.dbengine.client.query.current.data.IntPointRangeQuery;
import it.cavallium.dbengine.client.query.current.data.LongPointExactQuery;
import it.cavallium.dbengine.client.query.current.data.LongPointRangeQuery;
import it.cavallium.dbengine.client.query.current.data.NumericSort;
import it.cavallium.dbengine.client.query.current.data.PhraseQuery;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.SortedDocFieldExistsQuery;
import it.cavallium.dbengine.client.query.current.data.SortedNumericDocValuesFieldSlowRangeQuery;
import it.cavallium.dbengine.client.query.current.data.SynonymQuery;
import it.cavallium.dbengine.client.query.current.data.TermAndBoost;
import it.cavallium.dbengine.client.query.current.data.TermPosition;
import it.cavallium.dbengine.client.query.current.data.TermQuery;
import it.cavallium.dbengine.client.query.current.data.WildcardQuery;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSortField;

public class QueryParser {
	public static Query toQuery(it.cavallium.dbengine.client.query.current.data.Query query) {
		if (query == null) return null;
		switch (query.getBasicType$()) {
			case BooleanQuery:
				var booleanQuery = (it.cavallium.dbengine.client.query.current.data.BooleanQuery) query;
				var bq = new Builder();
				for (BooleanQueryPart part : booleanQuery.getParts()) {
					Occur occur;
					switch (part.getOccur().getBasicType$()) {
						case OccurFilter:
							occur = Occur.FILTER;
							break;
						case OccurMust:
							occur = Occur.MUST;
							break;
						case OccurShould:
							occur = Occur.SHOULD;
							break;
						case OccurMustNot:
							occur = Occur.MUST_NOT;
							break;
						default:
							throw new IllegalStateException("Unexpected value: " + part.getOccur().getBasicType$());
					}
					bq.add(toQuery(part.getQuery()), occur);
				}
				bq.setMinimumNumberShouldMatch(booleanQuery.getMinShouldMatch());
				return bq.build();
			case IntPointExactQuery:
				var intPointExactQuery = (IntPointExactQuery) query;
				return IntPoint.newExactQuery(intPointExactQuery.getField(), intPointExactQuery.getValue());
			case LongPointExactQuery:
				var longPointExactQuery = (LongPointExactQuery) query;
				return LongPoint.newExactQuery(longPointExactQuery.getField(), longPointExactQuery.getValue());
			case TermQuery:
				var termQuery = (TermQuery) query;
				return new org.apache.lucene.search.TermQuery(toTerm(termQuery.getTerm()));
			case BoostQuery:
				var boostQuery = (BoostQuery) query;
				return new org.apache.lucene.search.BoostQuery(toQuery(boostQuery.getQuery()), boostQuery.getScoreBoost());
			case ConstantScoreQuery:
				var constantScoreQuery = (ConstantScoreQuery) query;
				return new org.apache.lucene.search.ConstantScoreQuery(toQuery(constantScoreQuery.getQuery()));
			case BoxedQuery:
				return toQuery(((BoxedQuery) query).getQuery());
			case FuzzyQuery:
				var fuzzyQuery = (it.cavallium.dbengine.client.query.current.data.FuzzyQuery) query;
				return new FuzzyQuery(toTerm(fuzzyQuery.getTerm()),
						fuzzyQuery.getMaxEdits(),
						fuzzyQuery.getPrefixLength(),
						fuzzyQuery.getMaxExpansions(),
						fuzzyQuery.getTranspositions()
				);
			case IntPointRangeQuery:
				var intPointRangeQuery = (IntPointRangeQuery) query;
				return IntPoint.newRangeQuery(intPointRangeQuery.getField(),
						intPointRangeQuery.getMin(),
						intPointRangeQuery.getMax()
				);
			case LongPointRangeQuery:
				var longPointRangeQuery = (LongPointRangeQuery) query;
				return LongPoint.newRangeQuery(longPointRangeQuery.getField(),
						longPointRangeQuery.getMin(),
						longPointRangeQuery.getMax()
				);
			case MatchAllDocsQuery:
				return new MatchAllDocsQuery();
			case MatchNoDocsQuery:
				return new MatchNoDocsQuery();
			case PhraseQuery:
				var phraseQuery = (PhraseQuery) query;
				var pqb = new org.apache.lucene.search.PhraseQuery.Builder();
				for (TermPosition phrase : phraseQuery.getPhrase()) {
					pqb.add(toTerm(phrase.getTerm()), phrase.getPosition());
				}
				pqb.setSlop(phraseQuery.getSlop());
				return pqb.build();
			case SortedDocFieldExistsQuery:
				var sortedDocFieldExistsQuery = (SortedDocFieldExistsQuery) query;
				return new DocValuesFieldExistsQuery(sortedDocFieldExistsQuery.getField());
			case SynonymQuery:
				var synonymQuery = (SynonymQuery) query;
				var sqb = new org.apache.lucene.search.SynonymQuery.Builder(synonymQuery.getField());
				for (TermAndBoost part : synonymQuery.getParts()) {
					sqb.addTerm(toTerm(part.getTerm()), part.getBoost());
				}
				return sqb.build();
			case SortedNumericDocValuesFieldSlowRangeQuery:
				var sortedNumericDocValuesFieldSlowRangeQuery = (SortedNumericDocValuesFieldSlowRangeQuery) query;
				return SortedNumericDocValuesField.newSlowRangeQuery(sortedNumericDocValuesFieldSlowRangeQuery.getField(),
						sortedNumericDocValuesFieldSlowRangeQuery.getMin(),
						sortedNumericDocValuesFieldSlowRangeQuery.getMax()
				);
			case WildcardQuery:
				var wildcardQuery = (WildcardQuery) query;
				return new org.apache.lucene.search.WildcardQuery(new Term(wildcardQuery.getField(), wildcardQuery.getPattern()));
			default:
				throw new IllegalStateException("Unexpected value: " + query.getBasicType$());
		}
	}

	private static Term toTerm(it.cavallium.dbengine.client.query.current.data.Term term) {
		return new Term(term.getField(), term.getValue());
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
