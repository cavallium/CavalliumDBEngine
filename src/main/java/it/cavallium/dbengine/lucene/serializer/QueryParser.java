package it.cavallium.dbengine.lucene.serializer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;

public class QueryParser {

	static final boolean USE_PHRASE_QUERY = true;
	static final boolean USE_QUERY_BUILDER = true;

	public static Query parse(String text) throws ParseException {
		try {
			return (Query) parse(text, new AtomicInteger(0));
		} catch (Exception e) {
			throw new ParseException(e);
		}
	}

	public static Query parse(it.cavallium.dbengine.lucene.serializer.Query query) throws ParseException {
		StringBuilder sb = new StringBuilder();
		query.stringify(sb);
		return parse(sb.toString());
	}

	private static Object parse(String completeText, AtomicInteger position) {
		String text = completeText.substring(position.get());
		if (text.length() <= 2) {
			return null;
		}
		PrimitiveIterator.OfInt iterator = text.chars().iterator();
		StringBuilder numberBuilder = new StringBuilder();
		int index = 0;
		while (iterator.hasNext()) {
			char character = (char) iterator.nextInt();
			index++;
			if (character == '|') {
				break;
			} else {
				numberBuilder.append(character);
			}
		}
		int len = Integer.parseInt(numberBuilder.toString(), 16);
		StringBuilder typeBuilder = new StringBuilder();
		while (iterator.hasNext()) {
			char character = (char) iterator.nextInt();
			index++;
			if (character == '|') {
				break;
			} else {
				typeBuilder.append(character);
			}
		}
		QueryConstructorType type = QueryConstructorType.values()[Integer.parseInt(typeBuilder.toString())];

		position.addAndGet(index);

		String toParse = text.substring(index, index + len);
		switch (type) {
			case BOOST_QUERY:
				Query query = (Query) parse(completeText, position);
				Float numb = (Float) parse(completeText, position);
				assert query != null;
				assert numb != null;
				return new BoostQuery(query, numb);
			case FUZZY_QUERY:
				Term fqTerm = (Term) parse(completeText, position);
				Integer numb1 = (Integer) parse(completeText, position);
				Integer numb2 = (Integer) parse(completeText, position);
				Integer numb3 = (Integer) parse(completeText, position);
				Boolean bool1 = (Boolean) parse(completeText, position);
				assert fqTerm != null;
				assert numb1 != null;
				assert numb2 != null;
				assert numb3 != null;
				assert bool1 != null;
				return new FuzzyQuery(fqTerm, numb1, numb2, numb3, bool1);
			case PHRASE_QUERY:
				TermPosition[] pqTerms = (TermPosition[]) parse(completeText, position);
				var pqB = new PhraseQuery.Builder();
				assert pqTerms != null;
				for (TermPosition pqTerm : pqTerms) {
					if (pqTerm != null) {
						pqB.add(pqTerm.getTerm(), pqTerm.getPosition());
					}
				}
				Integer slops = Objects.requireNonNull((Integer) parse(completeText, position));
				pqB.setSlop(slops);
				return pqB.build();
			case SYNONYM_QUERY:
				var fieldName = (String) parse(completeText, position);
				var pqB2 = new SynonymQuery.Builder(fieldName);
				TermQuery[] pqTerms2
						= (TermQuery[]) parse(completeText, position);
				assert pqTerms2 != null;
				for (TermQuery pqTerm : pqTerms2) {
					if (pqTerm != null) {
						pqB2.addTerm(pqTerm.getTerm());
					}
				}
				return pqB2.build();
			case BOOLEAN_QUERY:
				var bqB = new BooleanQuery.Builder();
				//noinspection ConstantConditions
				int minShouldMatch = (Integer) parse(completeText, position);
				bqB.setMinimumNumberShouldMatch(minShouldMatch);
				BooleanQueryInfo[] bqTerms = (BooleanQueryInfo[]) parse(completeText, position);
				assert bqTerms != null;
				for (BooleanQueryInfo bqTerm : bqTerms) {
					bqB.add(bqTerm.query, bqTerm.occur);
				}
				return bqB.build();
			case BOOLEAN_QUERY_INFO:
				Query query1 = (Query) parse(completeText, position);
				BooleanClause.Occur occur = (BooleanClause.Occur) parse(completeText, position);
				return new BooleanQueryInfo(query1, occur);
			case INT_POINT_EXACT_QUERY:
				String string1 = (String) parse(completeText, position);
				Integer int1 = (Integer) parse(completeText, position);
				assert string1 != null;
				assert int1 != null;
				return IntPoint.newExactQuery(string1, int1);
			case LONG_POINT_EXACT_QUERY:
				String string5 = (String) parse(completeText, position);
				Long long3 = (Long) parse(completeText, position);
				assert string5 != null;
				assert long3 != null;
				return LongPoint.newExactQuery(string5, long3);
			case SORTED_SLOW_RANGE_QUERY:
				String string2 = (String) parse(completeText, position);
				Long long1 = (Long) parse(completeText, position);
				Long long2 = (Long) parse(completeText, position);
				assert string2 != null;
				assert long1 != null;
				assert long2 != null;
				return SortedNumericDocValuesField.newSlowRangeQuery(string2, long1, long2);
			case LONG_POINT_RANGE_QUERY:
				String stringX2 = (String) parse(completeText, position);
				Long longX1 = (Long) parse(completeText, position);
				Long longX2 = (Long) parse(completeText, position);
				assert stringX2 != null;
				assert longX1 != null;
				assert longX2 != null;
				return LongPoint.newRangeQuery(stringX2, longX1, longX2);
			case INT_POINT_RANGE_QUERY:
				String stringX3 = (String) parse(completeText, position);
				Integer intX1 = (Integer) parse(completeText, position);
				Integer intX2 = (Integer) parse(completeText, position);
				assert stringX3 != null;
				assert intX1 != null;
				assert intX2 != null;
				return IntPoint.newRangeQuery(stringX3, intX1, intX2);
			case INT:
				position.addAndGet(toParse.length());
				return Integer.parseInt(toParse);
			case LONG:
				position.addAndGet(toParse.length());
				return Long.parseLong(toParse);
			case TERM:
				String string3 = (String) parse(completeText, position);
				String string4 = (String) parse(completeText, position);
				assert string4 != null;
				return new Term(string3, string4);
			case TERM_POSITION:
				Term term1 = (Term) parse(completeText, position);
				Integer intX3 = (Integer) parse(completeText, position);
				assert intX3 != null;
				return new TermPosition(term1, intX3);
			case TERM_QUERY:
				Term term2 = (Term) parse(completeText, position);
				assert term2 != null;
				return new TermQuery(term2);
			case FLOAT:
				position.addAndGet(toParse.length());
				return Float.parseFloat(toParse);
			case STRING:
				position.addAndGet(toParse.length());
				return new String(Base64.getDecoder().decode(toParse), StandardCharsets.UTF_8);
			case BOOLEAN:
				position.addAndGet(toParse.length());
				return Boolean.parseBoolean(toParse);
			case NULL:
				position.addAndGet(toParse.length());
				return null;
			case TERM_POSITION_LIST:
				int termsCount;
				StringBuilder termsCountBuilder = new StringBuilder();
				var it = toParse.chars().iterator();
				while (it.hasNext()) {
					char character = (char) it.nextInt();
					position.incrementAndGet();
					if (character == '|') {
						break;
					} else {
						termsCountBuilder.append(character);
					}
				}
				termsCount = Integer.parseInt(termsCountBuilder.toString());

				var result1 = new TermPosition[termsCount];
				for (int i = 0; i < termsCount; i++) {
					result1[i] = (TermPosition) parse(completeText, position);
				}
				return result1;
			case TERM_QUERY_LIST:
				int termsCount2;
				StringBuilder termsCountBuilder2 = new StringBuilder();
				var it2 = toParse.chars().iterator();
				while (it2.hasNext()) {
					char character = (char) it2.nextInt();
					position.incrementAndGet();
					if (character == '|') {
						break;
					} else {
						termsCountBuilder2.append(character);
					}
				}
				termsCount2 = Integer.parseInt(termsCountBuilder2.toString());

				var result2 = new TermQuery[termsCount2];
				for (int i = 0; i < termsCount2; i++) {
					result2[i] = (TermQuery) parse(completeText, position);
				}
				return result2;
			case BOOLEAN_QUERY_INFO_LIST:
				int termsCount3;
				StringBuilder termsCountBuilder3 = new StringBuilder();
				var it3 = toParse.chars().iterator();
				while (it3.hasNext()) {
					char character = (char) it3.nextInt();
					position.incrementAndGet();
					if (character == '|') {
						break;
					} else {
						termsCountBuilder3.append(character);
					}
				}
				termsCount3 = Integer.parseInt(termsCountBuilder3.toString());

				var result3 = new BooleanQueryInfo[termsCount3];
				for (int i = 0; i < termsCount3; i++) {
					result3[i] = (BooleanQueryInfo) parse(completeText, position);
				}
				return result3;
			case OCCUR_MUST:
				return BooleanClause.Occur.MUST;
			case OCCUR_FILTER:
				return BooleanClause.Occur.FILTER;
			case OCCUR_SHOULD:
				return BooleanClause.Occur.SHOULD;
			case OCCUR_MUST_NOT:
				return BooleanClause.Occur.MUST_NOT;
			case MATCH_ALL_DOCS_QUERY:
				return new MatchAllDocsQuery();
			default:
				throw new UnsupportedOperationException("Unknown query constructor type: " + type);
		}
	}

	public static String stringify(SerializedQueryObject query) {
		StringBuilder sb = new StringBuilder();
		query.stringify(sb);
		return sb.toString();
	}
}
