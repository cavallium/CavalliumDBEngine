package it.cavallium.dbengine.client.query;

import it.cavallium.dbengine.client.query.current.data.BooleanQueryPart;
import it.cavallium.dbengine.client.query.current.data.BoostQuery;
import it.cavallium.dbengine.client.query.current.data.BoxedQuery;
import it.cavallium.dbengine.client.query.current.data.ConstantScoreQuery;
import it.cavallium.dbengine.client.query.current.data.DoubleNDPointExactQuery;
import it.cavallium.dbengine.client.query.current.data.DoubleNDPointRangeQuery;
import it.cavallium.dbengine.client.query.current.data.DoubleNDTermQuery;
import it.cavallium.dbengine.client.query.current.data.DoublePointExactQuery;
import it.cavallium.dbengine.client.query.current.data.DoublePointRangeQuery;
import it.cavallium.dbengine.client.query.current.data.DoublePointSetQuery;
import it.cavallium.dbengine.client.query.current.data.DoubleTermQuery;
import it.cavallium.dbengine.client.query.current.data.FieldExistsQuery;
import it.cavallium.dbengine.client.query.current.data.FloatNDPointExactQuery;
import it.cavallium.dbengine.client.query.current.data.FloatNDPointRangeQuery;
import it.cavallium.dbengine.client.query.current.data.FloatNDTermQuery;
import it.cavallium.dbengine.client.query.current.data.FloatPointExactQuery;
import it.cavallium.dbengine.client.query.current.data.FloatPointRangeQuery;
import it.cavallium.dbengine.client.query.current.data.FloatPointSetQuery;
import it.cavallium.dbengine.client.query.current.data.FloatTermQuery;
import it.cavallium.dbengine.client.query.current.data.IntNDPointExactQuery;
import it.cavallium.dbengine.client.query.current.data.IntNDPointRangeQuery;
import it.cavallium.dbengine.client.query.current.data.IntNDTermQuery;
import it.cavallium.dbengine.client.query.current.data.IntPointExactQuery;
import it.cavallium.dbengine.client.query.current.data.IntPointRangeQuery;
import it.cavallium.dbengine.client.query.current.data.IntPointSetQuery;
import it.cavallium.dbengine.client.query.current.data.IntTermQuery;
import it.cavallium.dbengine.client.query.current.data.LongNDPointExactQuery;
import it.cavallium.dbengine.client.query.current.data.LongNDPointRangeQuery;
import it.cavallium.dbengine.client.query.current.data.LongNDTermQuery;
import it.cavallium.dbengine.client.query.current.data.LongPointExactQuery;
import it.cavallium.dbengine.client.query.current.data.LongPointRangeQuery;
import it.cavallium.dbengine.client.query.current.data.LongPointSetQuery;
import it.cavallium.dbengine.client.query.current.data.LongTermQuery;
import it.cavallium.dbengine.client.query.current.data.NumericSort;
import it.cavallium.dbengine.client.query.current.data.PhraseQuery;
import it.cavallium.dbengine.client.query.current.data.PointConfig;
import it.cavallium.dbengine.client.query.current.data.PointType;
import it.cavallium.dbengine.client.query.current.data.SortedDocFieldExistsQuery;
import it.cavallium.dbengine.client.query.current.data.SortedNumericDocValuesFieldSlowRangeQuery;
import it.cavallium.dbengine.client.query.current.data.SynonymQuery;
import it.cavallium.dbengine.client.query.current.data.TermAndBoost;
import it.cavallium.dbengine.client.query.current.data.TermPosition;
import it.cavallium.dbengine.client.query.current.data.TermQuery;
import it.cavallium.dbengine.client.query.current.data.WildcardQuery;
import it.cavallium.dbengine.lucene.RandomSortField;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSortField;

public class QueryParser {

	public static Query toQuery(it.cavallium.dbengine.client.query.current.data.Query query, Analyzer analyzer) {
		if (query == null) {
			return null;
		}
		switch (query.getBaseType$()) {
			case StandardQuery:
				var standardQuery = (it.cavallium.dbengine.client.query.current.data.StandardQuery) query;

				// Fix the analyzer
				Map<String, Analyzer> customAnalyzers = standardQuery
						.termFields()
						.stream()
						.collect(Collectors.toMap(Function.identity(), term -> new NoOpAnalyzer()));
				analyzer = new PerFieldAnalyzerWrapper(analyzer, customAnalyzers);

				var standardQueryParser = new StandardQueryParser(analyzer);

				standardQueryParser.setPointsConfigMap(standardQuery
						.pointsConfig()
						.stream()
						.collect(Collectors.toMap(
								PointConfig::field,
								pointConfig -> new PointsConfig(
										toNumberFormat(pointConfig.data().numberFormat()),
										toType(pointConfig.data().type())
								)
						)));
				var defaultFields = standardQuery.defaultFields();
				try {
					Query parsed;
					if (defaultFields.size() > 1) {
						standardQueryParser.setMultiFields(defaultFields.toArray(String[]::new));
						parsed = standardQueryParser.parse(standardQuery.query(), null);
					} else if (defaultFields.size() == 1) {
						parsed = standardQueryParser.parse(standardQuery.query(), defaultFields.get(0));
					} else {
						throw new IllegalStateException("Can't parse a standard query expression that has 0 default fields");
					}
					return parsed;
				} catch (QueryNodeException e) {
					throw new IllegalStateException("Can't parse query expression \"" + standardQuery.query() + "\"", e);
				}
			case BooleanQuery:
				var booleanQuery = (it.cavallium.dbengine.client.query.current.data.BooleanQuery) query;
				var bq = new Builder();
				for (BooleanQueryPart part : booleanQuery.parts()) {
					Occur occur = switch (part.occur().getBaseType$()) {
						case OccurFilter -> Occur.FILTER;
						case OccurMust -> Occur.MUST;
						case OccurShould -> Occur.SHOULD;
						case OccurMustNot -> Occur.MUST_NOT;
						default -> throw new IllegalStateException("Unexpected value: " + part.occur().getBaseType$());
					};
					bq.add(toQuery(part.query(), analyzer), occur);
				}
				bq.setMinimumNumberShouldMatch(booleanQuery.minShouldMatch());
				return bq.build();
			case IntPointExactQuery:
				var intPointExactQuery = (IntPointExactQuery) query;
				return IntPoint.newExactQuery(intPointExactQuery.field(), intPointExactQuery.value());
			case IntNDPointExactQuery:
				var intndPointExactQuery = (IntNDPointExactQuery) query;
				var intndValues = intndPointExactQuery.value().toIntArray();
				return IntPoint.newRangeQuery(intndPointExactQuery.field(), intndValues, intndValues);
			case LongPointExactQuery:
				var longPointExactQuery = (LongPointExactQuery) query;
				return LongPoint.newExactQuery(longPointExactQuery.field(), longPointExactQuery.value());
			case FloatPointExactQuery:
				var floatPointExactQuery = (FloatPointExactQuery) query;
				return FloatPoint.newExactQuery(floatPointExactQuery.field(), floatPointExactQuery.value());
			case DoublePointExactQuery:
				var doublePointExactQuery = (DoublePointExactQuery) query;
				return DoublePoint.newExactQuery(doublePointExactQuery.field(), doublePointExactQuery.value());
			case LongNDPointExactQuery:
				var longndPointExactQuery = (LongNDPointExactQuery) query;
				var longndValues = longndPointExactQuery.value().toLongArray();
				return LongPoint.newRangeQuery(longndPointExactQuery.field(), longndValues, longndValues);
			case FloatNDPointExactQuery:
				var floatndPointExactQuery = (FloatNDPointExactQuery) query;
				var floatndValues = floatndPointExactQuery.value().toFloatArray();
				return FloatPoint.newRangeQuery(floatndPointExactQuery.field(), floatndValues, floatndValues);
			case DoubleNDPointExactQuery:
				var doublendPointExactQuery = (DoubleNDPointExactQuery) query;
				var doublendValues = doublendPointExactQuery.value().toDoubleArray();
				return DoublePoint.newRangeQuery(doublendPointExactQuery.field(), doublendValues, doublendValues);
			case IntPointSetQuery:
				var intPointSetQuery = (IntPointSetQuery) query;
				return IntPoint.newSetQuery(intPointSetQuery.field(), intPointSetQuery.values().toIntArray());
			case LongPointSetQuery:
				var longPointSetQuery = (LongPointSetQuery) query;
				return LongPoint.newSetQuery(longPointSetQuery.field(), longPointSetQuery.values().toLongArray());
			case FloatPointSetQuery:
				var floatPointSetQuery = (FloatPointSetQuery) query;
				return FloatPoint.newSetQuery(floatPointSetQuery.field(), floatPointSetQuery.values().toFloatArray());
			case DoublePointSetQuery:
				var doublePointSetQuery = (DoublePointSetQuery) query;
				return DoublePoint.newSetQuery(doublePointSetQuery.field(), doublePointSetQuery.values().toDoubleArray());
			case TermQuery:
				var termQuery = (TermQuery) query;
				return new org.apache.lucene.search.TermQuery(toTerm(termQuery.term()));
			case IntTermQuery:
				var intTermQuery = (IntTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(intTermQuery.field(),
						IntPoint.pack(intTermQuery.value())
				));
			case IntNDTermQuery:
				var intNDTermQuery = (IntNDTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(intNDTermQuery.field(),
						IntPoint.pack(intNDTermQuery.value().toIntArray())
				));
			case LongTermQuery:
				var longTermQuery = (LongTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(longTermQuery.field(),
						LongPoint.pack(longTermQuery.value())
				));
			case LongNDTermQuery:
				var longNDTermQuery = (LongNDTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(longNDTermQuery.field(),
						LongPoint.pack(longNDTermQuery.value().toLongArray())
				));
			case FloatTermQuery:
				var floatTermQuery = (FloatTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(floatTermQuery.field(),
						FloatPoint.pack(floatTermQuery.value())
				));
			case FloatNDTermQuery:
				var floatNDTermQuery = (FloatNDTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(floatNDTermQuery.field(),
						FloatPoint.pack(floatNDTermQuery.value().toFloatArray())
				));
			case DoubleTermQuery:
				var doubleTermQuery = (DoubleTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(doubleTermQuery.field(),
						DoublePoint.pack(doubleTermQuery.value())
				));
			case DoubleNDTermQuery:
				var doubleNDTermQuery = (DoubleNDTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(doubleNDTermQuery.field(),
						DoublePoint.pack(doubleNDTermQuery.value().toDoubleArray())
				));
			case FieldExistsQuery:
				var fieldExistQuery = (FieldExistsQuery) query;
				return new org.apache.lucene.search.FieldExistsQuery(fieldExistQuery.field());
			case BoostQuery:
				var boostQuery = (BoostQuery) query;
				return new org.apache.lucene.search.BoostQuery(toQuery(boostQuery.query(), analyzer), boostQuery.scoreBoost());
			case ConstantScoreQuery:
				var constantScoreQuery = (ConstantScoreQuery) query;
				return new org.apache.lucene.search.ConstantScoreQuery(toQuery(constantScoreQuery.query(), analyzer));
			case BoxedQuery:
				return toQuery(((BoxedQuery) query).query(), analyzer);
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
				return IntPoint.newRangeQuery(intPointRangeQuery.field(), intPointRangeQuery.min(), intPointRangeQuery.max());
			case IntNDPointRangeQuery:
				var intndPointRangeQuery = (IntNDPointRangeQuery) query;
				return IntPoint.newRangeQuery(intndPointRangeQuery.field(),
						intndPointRangeQuery.min().toIntArray(),
						intndPointRangeQuery.max().toIntArray()
				);
			case LongPointRangeQuery:
				var longPointRangeQuery = (LongPointRangeQuery) query;
				return LongPoint.newRangeQuery(longPointRangeQuery.field(),
						longPointRangeQuery.min(),
						longPointRangeQuery.max()
				);
			case FloatPointRangeQuery:
				var floatPointRangeQuery = (FloatPointRangeQuery) query;
				return FloatPoint.newRangeQuery(floatPointRangeQuery.field(),
						floatPointRangeQuery.min(),
						floatPointRangeQuery.max()
				);
			case DoublePointRangeQuery:
				var doublePointRangeQuery = (DoublePointRangeQuery) query;
				return DoublePoint.newRangeQuery(doublePointRangeQuery.field(),
						doublePointRangeQuery.min(),
						doublePointRangeQuery.max()
				);
			case LongNDPointRangeQuery:
				var longndPointRangeQuery = (LongNDPointRangeQuery) query;
				return LongPoint.newRangeQuery(longndPointRangeQuery.field(),
						longndPointRangeQuery.min().toLongArray(),
						longndPointRangeQuery.max().toLongArray()
				);
			case FloatNDPointRangeQuery:
				var floatndPointRangeQuery = (FloatNDPointRangeQuery) query;
				return FloatPoint.newRangeQuery(floatndPointRangeQuery.field(),
						floatndPointRangeQuery.min().toFloatArray(),
						floatndPointRangeQuery.max().toFloatArray()
				);
			case DoubleNDPointRangeQuery:
				var doublendPointRangeQuery = (DoubleNDPointRangeQuery) query;
				return DoublePoint.newRangeQuery(doublendPointRangeQuery.field(),
						doublendPointRangeQuery.min().toDoubleArray(),
						doublendPointRangeQuery.max().toDoubleArray()
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
				throw new IllegalStateException("Unexpected value: " + query.getBaseType$());
		}
	}

	private static NumberFormat toNumberFormat(it.cavallium.dbengine.client.query.current.data.NumberFormat numberFormat) {
		return switch (numberFormat.getBaseType$()) {
			case NumberFormatDecimal -> new DecimalFormat();
			default -> throw new UnsupportedOperationException("Unsupported type: " + numberFormat.getBaseType$());
		};
	}

	private static Class<? extends Number> toType(PointType type) {
		return switch (type.getBaseType$()) {
			case PointTypeInt -> Integer.class;
			case PointTypeLong -> Long.class;
			case PointTypeFloat -> Float.class;
			case PointTypeDouble -> Double.class;
			default -> throw new UnsupportedOperationException("Unsupported type: " + type.getBaseType$());
		};
	}

	private static Term toTerm(it.cavallium.dbengine.client.query.current.data.Term term) {
		return new Term(term.field(), term.value());
	}

	public static Sort toSort(it.cavallium.dbengine.client.query.current.data.Sort sort) {
		switch (sort.getBaseType$()) {
			case NoSort:
				return null;
			case ScoreSort:
				return new Sort(SortField.FIELD_SCORE);
			case DocSort:
				return new Sort(SortField.FIELD_DOC);
			case NumericSort:
				NumericSort numericSort = (NumericSort) sort;
				return new Sort(new SortedNumericSortField(numericSort.field(), Type.LONG, numericSort.reverse()));
			case RandomSort:
				return new Sort(new RandomSortField());
			default:
				throw new IllegalStateException("Unexpected value: " + sort.getBaseType$());
		}
	}

	public static it.cavallium.dbengine.client.query.current.data.Term toQueryTerm(Term term) {
		return it.cavallium.dbengine.client.query.current.data.Term.of(term.field(), term.text());
	}
}
