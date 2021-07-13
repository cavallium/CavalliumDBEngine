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
				for (BooleanQueryPart part : booleanQuery.parts()) {
					Occur occur = switch (part.occur().getBasicType$()) {
						case OccurFilter -> Occur.FILTER;
						case OccurMust -> Occur.MUST;
						case OccurShould -> Occur.SHOULD;
						case OccurMustNot -> Occur.MUST_NOT;
						default -> throw new IllegalStateException("Unexpected value: " + part.occur().getBasicType$());
					};
					bq.add(toQuery(part.query()), occur);
				}
				bq.setMinimumNumberShouldMatch(booleanQuery.minShouldMatch());
				return bq.build();
			case IntPointExactQuery:
				var intPointExactQuery = (IntPointExactQuery) query;
				return IntPoint.newExactQuery(intPointExactQuery.field(), intPointExactQuery.value());
			case LongPointExactQuery:
				var longPointExactQuery = (LongPointExactQuery) query;
				return LongPoint.newExactQuery(longPointExactQuery.field(), longPointExactQuery.value());
			case TermQuery:
				var termQuery = (TermQuery) query;
				return new org.apache.lucene.search.TermQuery(toTerm(termQuery.term()));
			case BoostQuery:
				var boostQuery = (BoostQuery) query;
				return new org.apache.lucene.search.BoostQuery(toQuery(boostQuery.query()), boostQuery.scoreBoost());
			case ConstantScoreQuery:
				var constantScoreQuery = (ConstantScoreQuery) query;
				return new org.apache.lucene.search.ConstantScoreQuery(toQuery(constantScoreQuery.query()));
			case BoxedQuery:
				return toQuery(((BoxedQuery) query).query());
			case FuzzyQuery:
				var fuzzyQuery = (it.cavallium.dbengine.client.query.current.data.FuzzyQuery) query;
				return new FuzzyQuery(toTerm(fuzzyQuery.term()),
						fuzzyQuery.maxEdits(),
						fuzzyQuery.prefixLength(),
						fuzzyQuery.maxExpansions(),
						fuzzyQuery.transpositions()
				);
			case IntPointRangeQuery:
				var intPointRangeQuery = (IntPointRangeQuery) query;
				return IntPoint.newRangeQuery(intPointRangeQuery.field(),
						intPointRangeQuery.min(),
						intPointRangeQuery.max()
				);
			case LongPointRangeQuery:
				var longPointRangeQuery = (LongPointRangeQuery) query;
				return LongPoint.newRangeQuery(longPointRangeQuery.field(),
						longPointRangeQuery.min(),
						longPointRangeQuery.max()
				);
			case MatchAllDocsQuery:
				return new MatchAllDocsQuery();
			case MatchNoDocsQuery:
				return new MatchNoDocsQuery();
			case PhraseQuery:
				var phraseQuery = (PhraseQuery) query;
				var pqb = new org.apache.lucene.search.PhraseQuery.Builder();
				for (TermPosition phrase : phraseQuery.phrase()) {
					pqb.add(toTerm(phrase.term()), phrase.position());
				}
				pqb.setSlop(phraseQuery.slop());
				return pqb.build();
			case SortedDocFieldExistsQuery:
				var sortedDocFieldExistsQuery = (SortedDocFieldExistsQuery) query;
				return new DocValuesFieldExistsQuery(sortedDocFieldExistsQuery.field());
			case SynonymQuery:
				var synonymQuery = (SynonymQuery) query;
				var sqb = new org.apache.lucene.search.SynonymQuery.Builder(synonymQuery.field());
				for (TermAndBoost part : synonymQuery.parts()) {
					sqb.addTerm(toTerm(part.term()), part.boost());
				}
				return sqb.build();
			case SortedNumericDocValuesFieldSlowRangeQuery:
				var sortedNumericDocValuesFieldSlowRangeQuery = (SortedNumericDocValuesFieldSlowRangeQuery) query;
				return SortedNumericDocValuesField.newSlowRangeQuery(sortedNumericDocValuesFieldSlowRangeQuery.field(),
						sortedNumericDocValuesFieldSlowRangeQuery.min(),
						sortedNumericDocValuesFieldSlowRangeQuery.max()
				);
			case WildcardQuery:
				var wildcardQuery = (WildcardQuery) query;
				return new org.apache.lucene.search.WildcardQuery(new Term(wildcardQuery.field(), wildcardQuery.pattern()));
			default:
				throw new IllegalStateException("Unexpected value: " + query.getBasicType$());
		}
	}

	private static Term toTerm(it.cavallium.dbengine.client.query.current.data.Term term) {
		return new Term(term.field(), term.value());
	}

	public static boolean isScoringEnabled(QueryParams queryParams) {
		return queryParams.scoreMode().computeScores();
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
				return new Sort(new SortedNumericSortField(numericSort.field(), Type.LONG, numericSort.reverse()));
			default:
				throw new IllegalStateException("Unexpected value: " + sort.getBasicType$());
		}
	}

	@SuppressWarnings("ConstantConditions")
	public static ScoreMode toScoreMode(it.cavallium.dbengine.client.query.current.data.ScoreMode scoreMode) {
		if (scoreMode.computeScores() && scoreMode.onlyTopScores()) {
			return ScoreMode.TOP_SCORES;
		} else if (scoreMode.computeScores() && !scoreMode.onlyTopScores()) {
			return ScoreMode.COMPLETE;
		} else if (!scoreMode.computeScores() && scoreMode.onlyTopScores()) {
			throw new IllegalStateException("Conflicting score mode options: [computeScores = false, onlyTopScore = true]");
		} else if (!scoreMode.computeScores() && !scoreMode.onlyTopScores()) {
			return ScoreMode.COMPLETE_NO_SCORES;
		} else {
			throw new IllegalStateException("Unexpected value: " + scoreMode);
		}
	}

	public static it.cavallium.dbengine.client.query.current.data.Term toQueryTerm(Term term) {
		return it.cavallium.dbengine.client.query.current.data.Term.of(term.field(), term.text());
	}
}
