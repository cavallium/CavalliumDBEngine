package it.cavallium.dbengine.client.query;

import com.google.common.xml.XmlEscapers;
import it.cavallium.dbengine.client.query.current.data.BooleanQuery;
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
import it.cavallium.dbengine.client.query.current.data.OccurShould;
import it.cavallium.dbengine.client.query.current.data.PhraseQuery;
import it.cavallium.dbengine.client.query.current.data.SolrTextQuery;
import it.cavallium.dbengine.client.query.current.data.SortedDocFieldExistsQuery;
import it.cavallium.dbengine.client.query.current.data.SynonymQuery;
import it.cavallium.dbengine.client.query.current.data.TermPosition;
import it.cavallium.dbengine.client.query.current.data.TermQuery;
import it.cavallium.dbengine.client.query.current.data.WildcardQuery;
import java.text.BreakIterator;
import java.util.Comparator;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

public class QueryParser {

	private static final String[] QUERY_STRING_FIND = {"\\", "\""};
	private static final String[] QUERY_STRING_REPLACE = {"\\\\", "\\\""};

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
		BreakIterator iterator = BreakIterator.getWordInstance(Locale.ENGLISH);
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

}
