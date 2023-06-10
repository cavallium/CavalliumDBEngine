package it.cavallium.dbengine.client.query;

import com.google.common.xml.XmlEscapers;
import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.util.ULocale;
import it.cavallium.dbengine.client.query.current.data.BooleanQuery;
import it.cavallium.dbengine.client.query.current.data.BooleanQueryBuilder;
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
import it.cavallium.dbengine.client.query.current.data.OccurMust;
import it.cavallium.dbengine.client.query.current.data.OccurMustNot;
import it.cavallium.dbengine.client.query.current.data.OccurShould;
import it.cavallium.dbengine.client.query.current.data.PhraseQuery;
import it.cavallium.dbengine.client.query.current.data.PointConfig;
import it.cavallium.dbengine.client.query.current.data.PointType;
import it.cavallium.dbengine.client.query.current.data.SolrTextQuery;
import it.cavallium.dbengine.client.query.current.data.SortedDocFieldExistsQuery;
import it.cavallium.dbengine.client.query.current.data.SortedNumericDocValuesFieldSlowRangeQuery;
import it.cavallium.dbengine.client.query.current.data.SynonymQuery;
import it.cavallium.dbengine.client.query.current.data.TermAndBoost;
import it.cavallium.dbengine.client.query.current.data.TermPosition;
import it.cavallium.dbengine.client.query.current.data.TermQuery;
import it.cavallium.dbengine.client.query.current.data.WildcardQuery;
import it.cavallium.dbengine.lucene.RandomSortField;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.icu.segmentation.DefaultICUTokenizerConfig;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.builders.UserInputQueryBuilder;
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
import org.jetbrains.annotations.Nullable;

public class QueryParser {

	private static final String[] QUERY_STRING_FIND = {"\\", "\""};
	private static final String[] QUERY_STRING_REPLACE = {"\\\\", "\\\""};

	public static Query toQuery(it.cavallium.dbengine.client.query.current.data.Query query, Analyzer analyzer) {
		if (query == null) {
			return null;
		}
		switch (query.getBaseType$()) {
			case StandardQuery -> {
				var standardQuery = (it.cavallium.dbengine.client.query.current.data.StandardQuery) query;

				// Fix the analyzer
				Map<String, Analyzer> customAnalyzers = standardQuery
						.termFields()
						.stream()
						.collect(Collectors.toMap(Function.identity(), term -> new NoOpAnalyzer()));
				analyzer = new PerFieldAnalyzerWrapper(analyzer, customAnalyzers);
				var standardQueryParser = new StandardQueryParser(analyzer);
				standardQueryParser.setPointsConfigMap(standardQuery.pointsConfig().stream().collect(
						Collectors.toMap(PointConfig::field, pointConfig ->
								new PointsConfig(toNumberFormat(pointConfig.data().numberFormat()), toType(pointConfig.data().type()))
						))
				);
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
			}
			case BooleanQuery -> {
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
			}
			case IntPointExactQuery -> {
				var intPointExactQuery = (IntPointExactQuery) query;
				return IntPoint.newExactQuery(intPointExactQuery.field(), intPointExactQuery.value());
			}
			case IntNDPointExactQuery -> {
				var intndPointExactQuery = (IntNDPointExactQuery) query;
				var intndValues = intndPointExactQuery.value().toIntArray();
				return IntPoint.newRangeQuery(intndPointExactQuery.field(), intndValues, intndValues);
			}
			case LongPointExactQuery -> {
				var longPointExactQuery = (LongPointExactQuery) query;
				return LongPoint.newExactQuery(longPointExactQuery.field(), longPointExactQuery.value());
			}
			case FloatPointExactQuery -> {
				var floatPointExactQuery = (FloatPointExactQuery) query;
				return FloatPoint.newExactQuery(floatPointExactQuery.field(), floatPointExactQuery.value());
			}
			case DoublePointExactQuery -> {
				var doublePointExactQuery = (DoublePointExactQuery) query;
				return DoublePoint.newExactQuery(doublePointExactQuery.field(), doublePointExactQuery.value());
			}
			case LongNDPointExactQuery -> {
				var longndPointExactQuery = (LongNDPointExactQuery) query;
				var longndValues = longndPointExactQuery.value().toLongArray();
				return LongPoint.newRangeQuery(longndPointExactQuery.field(), longndValues, longndValues);
			}
			case FloatNDPointExactQuery -> {
				var floatndPointExactQuery = (FloatNDPointExactQuery) query;
				var floatndValues = floatndPointExactQuery.value().toFloatArray();
				return FloatPoint.newRangeQuery(floatndPointExactQuery.field(), floatndValues, floatndValues);
			}
			case DoubleNDPointExactQuery -> {
				var doublendPointExactQuery = (DoubleNDPointExactQuery) query;
				var doublendValues = doublendPointExactQuery.value().toDoubleArray();
				return DoublePoint.newRangeQuery(doublendPointExactQuery.field(), doublendValues, doublendValues);
			}
			case IntPointSetQuery -> {
				var intPointSetQuery = (IntPointSetQuery) query;
				return IntPoint.newSetQuery(intPointSetQuery.field(), intPointSetQuery.values().toIntArray());
			}
			case LongPointSetQuery -> {
				var longPointSetQuery = (LongPointSetQuery) query;
				return LongPoint.newSetQuery(longPointSetQuery.field(), longPointSetQuery.values().toLongArray());
			}
			case FloatPointSetQuery -> {
				var floatPointSetQuery = (FloatPointSetQuery) query;
				return FloatPoint.newSetQuery(floatPointSetQuery.field(), floatPointSetQuery.values().toFloatArray());
			}
			case DoublePointSetQuery -> {
				var doublePointSetQuery = (DoublePointSetQuery) query;
				return DoublePoint.newSetQuery(doublePointSetQuery.field(), doublePointSetQuery.values().toDoubleArray());
			}
			case TermQuery -> {
				var termQuery = (TermQuery) query;
				return new org.apache.lucene.search.TermQuery(toTerm(termQuery.term()));
			}
			case IntTermQuery -> {
				var intTermQuery = (IntTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(intTermQuery.field(),
						IntPoint.pack(intTermQuery.value())
				));
			}
			case IntNDTermQuery -> {
				var intNDTermQuery = (IntNDTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(intNDTermQuery.field(),
						IntPoint.pack(intNDTermQuery.value().toIntArray())
				));
			}
			case LongTermQuery -> {
				var longTermQuery = (LongTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(longTermQuery.field(),
						LongPoint.pack(longTermQuery.value())
				));
			}
			case LongNDTermQuery -> {
				var longNDTermQuery = (LongNDTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(longNDTermQuery.field(),
						LongPoint.pack(longNDTermQuery.value().toLongArray())
				));
			}
			case FloatTermQuery -> {
				var floatTermQuery = (FloatTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(floatTermQuery.field(),
						FloatPoint.pack(floatTermQuery.value())
				));
			}
			case FloatNDTermQuery -> {
				var floatNDTermQuery = (FloatNDTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(floatNDTermQuery.field(),
						FloatPoint.pack(floatNDTermQuery.value().toFloatArray())
				));
			}
			case DoubleTermQuery -> {
				var doubleTermQuery = (DoubleTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(doubleTermQuery.field(),
						DoublePoint.pack(doubleTermQuery.value())
				));
			}
			case DoubleNDTermQuery -> {
				var doubleNDTermQuery = (DoubleNDTermQuery) query;
				return new org.apache.lucene.search.TermQuery(new Term(doubleNDTermQuery.field(),
						DoublePoint.pack(doubleNDTermQuery.value().toDoubleArray())
				));
			}
			case FieldExistsQuery -> {
				var fieldExistQuery = (FieldExistsQuery) query;
				return new org.apache.lucene.search.FieldExistsQuery(fieldExistQuery.field());
			}
			case BoostQuery -> {
				var boostQuery = (BoostQuery) query;
				return new org.apache.lucene.search.BoostQuery(toQuery(boostQuery.query(), analyzer), boostQuery.scoreBoost());
			}
			case ConstantScoreQuery -> {
				var constantScoreQuery = (ConstantScoreQuery) query;
				return new org.apache.lucene.search.ConstantScoreQuery(toQuery(constantScoreQuery.query(), analyzer));
			}
			case BoxedQuery -> {
				return toQuery(((BoxedQuery) query).query(), analyzer);
			}
			case FuzzyQuery -> {
				var fuzzyQuery = (it.cavallium.dbengine.client.query.current.data.FuzzyQuery) query;
				return new FuzzyQuery(toTerm(fuzzyQuery.term()),
						fuzzyQuery.maxEdits(),
						fuzzyQuery.prefixLength(),
						fuzzyQuery.maxExpansions(),
						fuzzyQuery.transpositions()
				);
			}
			case IntPointRangeQuery -> {
				var intPointRangeQuery = (IntPointRangeQuery) query;
				return IntPoint.newRangeQuery(intPointRangeQuery.field(), intPointRangeQuery.min(), intPointRangeQuery.max());
			}
			case IntNDPointRangeQuery -> {
				var intndPointRangeQuery = (IntNDPointRangeQuery) query;
				return IntPoint.newRangeQuery(intndPointRangeQuery.field(),
						intndPointRangeQuery.min().toIntArray(),
						intndPointRangeQuery.max().toIntArray()
				);
			}
			case LongPointRangeQuery -> {
				var longPointRangeQuery = (LongPointRangeQuery) query;
				return LongPoint.newRangeQuery(longPointRangeQuery.field(),
						longPointRangeQuery.min(),
						longPointRangeQuery.max()
				);
			}
			case FloatPointRangeQuery -> {
				var floatPointRangeQuery = (FloatPointRangeQuery) query;
				return FloatPoint.newRangeQuery(floatPointRangeQuery.field(),
						floatPointRangeQuery.min(),
						floatPointRangeQuery.max()
				);
			}
			case DoublePointRangeQuery -> {
				var doublePointRangeQuery = (DoublePointRangeQuery) query;
				return DoublePoint.newRangeQuery(doublePointRangeQuery.field(),
						doublePointRangeQuery.min(),
						doublePointRangeQuery.max()
				);
			}
			case LongNDPointRangeQuery -> {
				var longndPointRangeQuery = (LongNDPointRangeQuery) query;
				return LongPoint.newRangeQuery(longndPointRangeQuery.field(),
						longndPointRangeQuery.min().toLongArray(),
						longndPointRangeQuery.max().toLongArray()
				);
			}
			case FloatNDPointRangeQuery -> {
				var floatndPointRangeQuery = (FloatNDPointRangeQuery) query;
				return FloatPoint.newRangeQuery(floatndPointRangeQuery.field(),
						floatndPointRangeQuery.min().toFloatArray(),
						floatndPointRangeQuery.max().toFloatArray()
				);
			}
			case DoubleNDPointRangeQuery -> {
				var doublendPointRangeQuery = (DoubleNDPointRangeQuery) query;
				return DoublePoint.newRangeQuery(doublendPointRangeQuery.field(),
						doublendPointRangeQuery.min().toDoubleArray(),
						doublendPointRangeQuery.max().toDoubleArray()
				);
			}
			case MatchAllDocsQuery -> {
				return new MatchAllDocsQuery();
			}
			case MatchNoDocsQuery -> {
				return new MatchNoDocsQuery();
			}
			case PhraseQuery -> {
				var phraseQuery = (PhraseQuery) query;
				var pqb = new org.apache.lucene.search.PhraseQuery.Builder();
				for (TermPosition phrase : phraseQuery.phrase()) {
					pqb.add(toTerm(phrase.term()), phrase.position());
				}
				pqb.setSlop(phraseQuery.slop());
				return pqb.build();
			}
			case SortedDocFieldExistsQuery -> {
				var sortedDocFieldExistsQuery = (SortedDocFieldExistsQuery) query;
				return new DocValuesFieldExistsQuery(sortedDocFieldExistsQuery.field());
			}
			case SynonymQuery -> {
				var synonymQuery = (SynonymQuery) query;
				var sqb = new org.apache.lucene.search.SynonymQuery.Builder(synonymQuery.field());
				for (TermAndBoost part : synonymQuery.parts()) {
					sqb.addTerm(toTerm(part.term()), part.boost());
				}
				return sqb.build();
			}
			case SortedNumericDocValuesFieldSlowRangeQuery -> {
				var sortedNumericDocValuesFieldSlowRangeQuery = (SortedNumericDocValuesFieldSlowRangeQuery) query;
				return SortedNumericDocValuesField.newSlowRangeQuery(sortedNumericDocValuesFieldSlowRangeQuery.field(),
						sortedNumericDocValuesFieldSlowRangeQuery.min(),
						sortedNumericDocValuesFieldSlowRangeQuery.max()
				);
			}
			case WildcardQuery -> {
				var wildcardQuery = (WildcardQuery) query;
				return new org.apache.lucene.search.WildcardQuery(new Term(wildcardQuery.field(), wildcardQuery.pattern()));
			}
			default -> throw new IllegalStateException("Unexpected value: " + query.getBaseType$());
		}
	}

	public static void toQueryXML(StringBuilder out,
			it.cavallium.dbengine.client.query.current.data.Query query,
			@Nullable Float boost) {
		if (query == null) {
			return;
		}
		switch (query.getBaseType$()) {
			case StandardQuery -> {
				var standardQuery = (it.cavallium.dbengine.client.query.current.data.StandardQuery) query;

				out.append("<UserQuery");
				if (standardQuery.defaultFields().size() > 1) {
					throw new UnsupportedOperationException("Maximum supported default fields count: 1");
				}
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				if (standardQuery.defaultFields().size() == 1) {
					out
							.append(" fieldName=\"")
							.append(XmlEscapers.xmlAttributeEscaper().escape(standardQuery.defaultFields().get(0)))
							.append("\"");
				}
				if (!standardQuery.termFields().isEmpty()) {
					throw new UnsupportedOperationException("Term fields unsupported");
				}
				if (!standardQuery.pointsConfig().isEmpty()) {
					throw new UnsupportedOperationException("Points config unsupported");
				}
				out.append(">");
				out.append(XmlEscapers.xmlContentEscaper().escape(standardQuery.query()));
				out.append("</UserQuery>\n");
			}
			case BooleanQuery -> {
				var booleanQuery = (it.cavallium.dbengine.client.query.current.data.BooleanQuery) query;
				if (booleanQuery.parts().size() == 1
						&& booleanQuery.parts().get(0).occur().getBaseType$() == BaseType.OccurMust) {
					toQueryXML(out, booleanQuery.parts().get(0).query(), boost);
				} else {
					out.append("<BooleanQuery");
					if (boost != null) {
						out.append(" boost=\"").append(boost).append("\"");
					}
					out.append(" minimumNumberShouldMatch=\"").append(booleanQuery.minShouldMatch()).append("\"");
					out.append(">\n");

					for (BooleanQueryPart part : booleanQuery.parts()) {
						out.append("<Clause");
						out.append(" occurs=\"").append(switch (part.occur().getBaseType$()) {
							case OccurFilter -> "filter";
							case OccurMust -> "must";
							case OccurShould -> "should";
							case OccurMustNot -> "mustNot";
							default -> throw new IllegalStateException("Unexpected value: " + part.occur().getBaseType$());
						}).append("\"");
						out.append(">\n");
						toQueryXML(out, part.query(), null);
						out.append("</Clause>\n");
					}
					out.append("</BooleanQuery>\n");
				}
			}
			case IntPointExactQuery -> {
				var intPointExactQuery = (IntPointExactQuery) query;
				out.append("<PointRangeQuery type=\"int\"");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(" fieldName=\"").append(XmlEscapers.xmlAttributeEscaper().escape(intPointExactQuery.field())).append("\"");
				out.append(" lowerTerm=\"").append(intPointExactQuery.value()).append("\"");
				out.append(" upperTerm=\"").append(intPointExactQuery.value()).append("\"");
				out.append(" />\n");
			}
			case IntNDPointExactQuery -> {
				var intPointExactQuery = (IntPointExactQuery) query;
				throw new UnsupportedOperationException("N-dimensional point queries are not supported");
			}
			case LongPointExactQuery -> {
				var longPointExactQuery = (LongPointExactQuery) query;
				out.append("<PointRangeQuery type=\"long\"");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(" fieldName=\"").append(XmlEscapers.xmlAttributeEscaper().escape(longPointExactQuery.field())).append("\"");
				out.append(" lowerTerm=\"").append(longPointExactQuery.value()).append("\"");
				out.append(" upperTerm=\"").append(longPointExactQuery.value()).append("\"");
				out.append(" />\n");
			}
			case FloatPointExactQuery -> {
				var floatPointExactQuery = (FloatPointExactQuery) query;
				out.append("<PointRangeQuery type=\"float\"");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(" fieldName=\"").append(XmlEscapers.xmlAttributeEscaper().escape(floatPointExactQuery.field())).append("\"");
				out.append(" lowerTerm=\"").append(floatPointExactQuery.value()).append("\"");
				out.append(" upperTerm=\"").append(floatPointExactQuery.value()).append("\"");
				out.append(" />\n");
			}
			case DoublePointExactQuery -> {
				var doublePointExactQuery = (DoublePointExactQuery) query;
				out.append("<PointRangeQuery type=\"double\"");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(" fieldName=\"").append(XmlEscapers.xmlAttributeEscaper().escape(doublePointExactQuery.field())).append("\"");
				out.append(" lowerTerm=\"").append(doublePointExactQuery.value()).append("\"");
				out.append(" upperTerm=\"").append(doublePointExactQuery.value()).append("\"");
				out.append(" />\n");
			}
			case LongNDPointExactQuery -> {
				var longndPointExactQuery = (LongNDPointExactQuery) query;
				throw new UnsupportedOperationException("N-dimensional point queries are not supported");
			}
			case FloatNDPointExactQuery -> {
				var floatndPointExactQuery = (FloatNDPointExactQuery) query;
				throw new UnsupportedOperationException("N-dimensional point queries are not supported");
			}
			case DoubleNDPointExactQuery -> {
				var doublendPointExactQuery = (DoubleNDPointExactQuery) query;
				throw new UnsupportedOperationException("N-dimensional point queries are not supported");
			}
			case IntPointSetQuery -> {
				var intPointSetQuery = (IntPointSetQuery) query;
				// Polyfill
				toQueryXML(out, BooleanQuery.of(intPointSetQuery.values().intStream()
						.mapToObj(val -> IntPointExactQuery.of(intPointSetQuery.field(), val))
						.map(q -> BooleanQueryPart.of(q, OccurShould.of()))
						.toList(), 1), boost);
			}
			case LongPointSetQuery -> {
				var longPointSetQuery = (LongPointSetQuery) query;
				// Polyfill
				toQueryXML(out, BooleanQuery.of(longPointSetQuery.values().longStream()
						.mapToObj(val -> LongPointExactQuery.of(longPointSetQuery.field(), val))
						.map(q -> BooleanQueryPart.of(q, OccurShould.of()))
						.toList(), 1), boost);
			}
			case FloatPointSetQuery -> {
				var floatPointSetQuery = (FloatPointSetQuery) query;
				// Polyfill
				toQueryXML(out, BooleanQuery.of(floatPointSetQuery.values().stream()
						.map(val -> FloatPointExactQuery.of(floatPointSetQuery.field(), val))
						.map(q -> BooleanQueryPart.of(q, OccurShould.of()))
						.toList(), 1), boost);
			}
			case DoublePointSetQuery -> {
				var doublePointSetQuery = (DoublePointSetQuery) query;
				// Polyfill
				toQueryXML(out, BooleanQuery.of(doublePointSetQuery.values().doubleStream()
						.mapToObj(val -> DoublePointExactQuery.of(doublePointSetQuery.field(), val))
						.map(q -> BooleanQueryPart.of(q, OccurShould.of()))
						.toList(), 1), boost);
			}
			case TermQuery -> {
				var termQuery = (TermQuery) query;
				out
						.append("<TermQuery");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out
						.append(" fieldName=\"")
						.append(XmlEscapers.xmlAttributeEscaper().escape(termQuery.term().field()))
						.append("\"");
				out.append(">");
				out.append(XmlEscapers.xmlContentEscaper().escape(termQuery.term().value()));
				out.append("</TermQuery>\n");
			}
			case IntTermQuery -> {
				var intTermQuery = (IntTermQuery) query;
				throw new UnsupportedOperationException("Non-string term fields are not supported");
			}
			case IntNDTermQuery -> {
				var intNDTermQuery = (IntNDTermQuery) query;
				throw new UnsupportedOperationException("Non-string term fields are not supported");
			}
			case LongTermQuery -> {
				var longTermQuery = (LongTermQuery) query;
				throw new UnsupportedOperationException("Non-string term fields are not supported");
			}
			case LongNDTermQuery -> {
				var longNDTermQuery = (LongNDTermQuery) query;
				throw new UnsupportedOperationException("Non-string term fields are not supported");
			}
			case FloatTermQuery -> {
				var floatTermQuery = (FloatTermQuery) query;
				throw new UnsupportedOperationException("Non-string term fields are not supported");
			}
			case FloatNDTermQuery -> {
				var floatNDTermQuery = (FloatNDTermQuery) query;
				throw new UnsupportedOperationException("Non-string term fields are not supported");
			}
			case DoubleTermQuery -> {
				var doubleTermQuery = (DoubleTermQuery) query;
				throw new UnsupportedOperationException("Non-string term fields are not supported");
			}
			case DoubleNDTermQuery -> {
				var doubleNDTermQuery = (DoubleNDTermQuery) query;
				throw new UnsupportedOperationException("Non-string term fields are not supported");
			}
			case FieldExistsQuery -> {
				var fieldExistQuery = (FieldExistsQuery) query;
				out.append("<UserQuery");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(">");
				ensureValidField(fieldExistQuery.field());
				out.append(fieldExistQuery.field());
				out.append(":[* TO *]");
				out.append("</UserQuery>\n");
			}
			case SolrTextQuery -> {
				var solrTextQuery = (SolrTextQuery) query;
				out.append("<UserQuery");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(">");
				ensureValidField(solrTextQuery.field());
				out.append(solrTextQuery.field());
				out.append(":");
				out.append("\"").append(XmlEscapers.xmlContentEscaper().escape(escapeQueryStringValue(solrTextQuery.phrase()))).append("\"");
				if (solrTextQuery.slop() > 0 && hasMoreThanOneWord(solrTextQuery.phrase())) {
					out.append("~").append(solrTextQuery.slop());
				}
				out.append("</UserQuery>\n");
			}
			case BoostQuery -> {
				var boostQuery = (BoostQuery) query;
				toQueryXML(out, boostQuery.query(), boostQuery.scoreBoost());
			}
			case ConstantScoreQuery -> {
				var constantScoreQuery = (ConstantScoreQuery) query;
				out.append("<ConstantScoreQuery");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(">\n");
				toQueryXML(out, query, null);
				out.append("</ConstantScoreQuery>\n");
			}
			case BoxedQuery -> {
				toQueryXML(out, ((BoxedQuery) query).query(), boost);
			}
			case FuzzyQuery -> {
				var fuzzyQuery = (it.cavallium.dbengine.client.query.current.data.FuzzyQuery) query;
				new FuzzyQuery(toTerm(fuzzyQuery.term()),
						fuzzyQuery.maxEdits(),
						fuzzyQuery.prefixLength(),
						fuzzyQuery.maxExpansions(),
						fuzzyQuery.transpositions()
				);
				throw new UnsupportedOperationException("Fuzzy query is not supported, use span queries");
			}
			case IntPointRangeQuery -> {
				var intPointRangeQuery = (IntPointRangeQuery) query;
				out.append("<PointRangeQuery type=\"int\"");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(" fieldName=\"").append(XmlEscapers.xmlAttributeEscaper().escape(intPointRangeQuery.field())).append("\"");
				out.append(" lowerTerm=\"").append(intPointRangeQuery.min()).append("\"");
				out.append(" upperTerm=\"").append(intPointRangeQuery.max()).append("\"");
				out.append(" />\n");
			}
			case IntNDPointRangeQuery -> {
				var intndPointRangeQuery = (IntNDPointRangeQuery) query;
				throw new UnsupportedOperationException("N-dimensional point queries are not supported");
			}
			case LongPointRangeQuery -> {
				var longPointRangeQuery = (LongPointRangeQuery) query;
				out.append("<PointRangeQuery type=\"long\"");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(" fieldName=\"").append(XmlEscapers.xmlAttributeEscaper().escape(longPointRangeQuery.field())).append("\"");
				out.append(" lowerTerm=\"").append(longPointRangeQuery.min()).append("\"");
				out.append(" upperTerm=\"").append(longPointRangeQuery.max()).append("\"");
				out.append(" />\n");
			}
			case FloatPointRangeQuery -> {
				var floatPointRangeQuery = (FloatPointRangeQuery) query;
				out.append("<PointRangeQuery type=\"float\"");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(" fieldName=\"").append(XmlEscapers.xmlAttributeEscaper().escape(floatPointRangeQuery.field())).append("\"");
				out.append(" lowerTerm=\"").append(floatPointRangeQuery.min()).append("\"");
				out.append(" upperTerm=\"").append(floatPointRangeQuery.max()).append("\"");
				out.append(" />\n");
			}
			case DoublePointRangeQuery -> {
				var doublePointRangeQuery = (DoublePointRangeQuery) query;
				out.append("<PointRangeQuery type=\"double\"");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(" fieldName=\"").append(XmlEscapers.xmlAttributeEscaper().escape(doublePointRangeQuery.field())).append("\"");
				out.append(" lowerTerm=\"").append(doublePointRangeQuery.min()).append("\"");
				out.append(" upperTerm=\"").append(doublePointRangeQuery.max()).append("\"");
				out.append(" />\n");
			}
			case LongNDPointRangeQuery -> {
				var longndPointRangeQuery = (LongNDPointRangeQuery) query;
				throw new UnsupportedOperationException("N-dimensional point queries are not supported");
			}
			case FloatNDPointRangeQuery -> {
				var floatndPointRangeQuery = (FloatNDPointRangeQuery) query;
				throw new UnsupportedOperationException("N-dimensional point queries are not supported");
			}
			case DoubleNDPointRangeQuery -> {
				var doublendPointRangeQuery = (DoubleNDPointRangeQuery) query;
				throw new UnsupportedOperationException("N-dimensional point queries are not supported");
			}
			case MatchAllDocsQuery -> {
				out.append("<UserQuery");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(">");
				out.append("*:*");
				out.append("</UserQuery>\n");
			}
			case MatchNoDocsQuery -> {
				out.append("<UserQuery");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(">");
				//todo: check if it's correct
				out.append("!*:*");
				out.append("</UserQuery>\n");
			}
			case PhraseQuery -> {
				//todo: check if it's correct

				var phraseQuery = (PhraseQuery) query;
				out.append("<SpanNear");
				if (boost != null) {
					out.append(" boost=\"").append(boost).append("\"");
				}
				out.append(" inOrder=\"true\"");
				out.append(">\n");
				phraseQuery.phrase().stream().sorted(Comparator.comparingInt(TermPosition::position)).forEach(term -> {
					out
							.append("<SpanTerm fieldName=\"")
							.append(XmlEscapers.xmlAttributeEscaper().escape(term.term().field()))
							.append("\">")
							.append(XmlEscapers.xmlContentEscaper().escape(term.term().value()))
							.append("</SpanTerm>\n");
				});
				out.append("</SpanNear>\n");
			}
			case SortedDocFieldExistsQuery -> {
				var sortedDocFieldExistsQuery = (SortedDocFieldExistsQuery) query;
				throw new UnsupportedOperationException("Field existence query is not supported");
			}
			case SynonymQuery -> {
				var synonymQuery = (SynonymQuery) query;
				throw new UnsupportedOperationException("Synonym query is not supported");
			}
			case SortedNumericDocValuesFieldSlowRangeQuery -> {
				throw new UnsupportedOperationException("Slow range query is not supported");
			}
			case WildcardQuery -> {
				var wildcardQuery = (WildcardQuery) query;
				throw new UnsupportedOperationException("Wildcard query is not supported");
			}
			default -> throw new IllegalStateException("Unexpected value: " + query.getBaseType$());
		}
	}

	private static boolean hasMoreThanOneWord(String sentence) {
		BreakIterator iterator = BreakIterator.getWordInstance(ULocale.ENGLISH);
		iterator.setText(sentence);

		boolean firstWord = false;
		iterator.first();
		int end = iterator.next();
		while (end != BreakIterator.DONE) {
			if (!firstWord) {
				firstWord = true;
			} else {
				return true;
			}
			end = iterator.next();
		}
		return false;
	}

	private static String escapeQueryStringValue(String text) {
		return StringUtils.replaceEach(text, QUERY_STRING_FIND, QUERY_STRING_REPLACE);
	}

	private static void ensureValidField(String field) {
		field.codePoints().forEach(codePoint -> {
			if (!Character.isLetterOrDigit(codePoint) && codePoint != '_') {
				throw new UnsupportedOperationException(
						"Invalid character \"" + codePoint + "\" in field name \"" + field + "\"");
			}
		});
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
