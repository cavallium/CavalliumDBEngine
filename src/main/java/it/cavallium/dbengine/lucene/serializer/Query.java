package it.cavallium.dbengine.lucene.serializer;

import static it.cavallium.dbengine.lucene.serializer.QueryParser.USE_PHRASE_QUERY;
import static it.cavallium.dbengine.lucene.serializer.QueryParser.USE_QUERY_BUILDER;

import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.util.QueryBuilder;
import org.jetbrains.annotations.NotNull;

public interface Query extends SerializedQueryObject {

	static Query approximateSearch(TextFieldsAnalyzer preferredAnalyzer, String field, String text) {
		if (USE_QUERY_BUILDER) {
			var qb = new QueryBuilder(LuceneUtils.getAnalyzer(preferredAnalyzer));
			var luceneQuery = qb.createMinShouldMatchQuery(field, text, 0.75f);
			return transformQuery(field, luceneQuery);
		}

		try {
			var terms = getTerms(preferredAnalyzer, field, text);

			List<BooleanQueryPart> booleanQueryParts = new LinkedList<>();
			for (TermPosition term : terms) {
				booleanQueryParts.add(new BooleanQueryPart(new TermQuery(term.getTerm()), Occur.MUST));
				booleanQueryParts.add(new BooleanQueryPart(new PhraseQuery(terms.toArray(TermPosition[]::new)), Occur.SHOULD));
			}
			return new BooleanQuery(booleanQueryParts);
		} catch (IOException e) {
			e.printStackTrace();
			return exactSearch(preferredAnalyzer, field, text);
		}
	}

	static Query exactSearch(TextFieldsAnalyzer preferredAnalyzer, String field, String text) {
		if (USE_QUERY_BUILDER) {
			var qb = new QueryBuilder(LuceneUtils.getAnalyzer(preferredAnalyzer));
			var luceneQuery = qb.createPhraseQuery(field, text);
			return transformQuery(field, luceneQuery);
		}

		try {
			var terms = getTerms(preferredAnalyzer, field, text);

			if (USE_PHRASE_QUERY) {
				return new PhraseQuery(terms.toArray(TermPosition[]::new));
			} else {
				List<BooleanQueryPart> booleanQueryParts = new LinkedList<>();
				for (TermPosition term : terms) {
					booleanQueryParts.add(new BooleanQueryPart(new TermQuery(term.getTerm()), Occur.MUST));
				}
				booleanQueryParts.add(new BooleanQueryPart(new PhraseQuery(terms.toArray(TermPosition[]::new)), Occur.FILTER));
				return new BooleanQuery(booleanQueryParts);
			}
		} catch (IOException exception) {
			throw new RuntimeException(exception);
		}
	}

	@NotNull
	private static Query transformQuery(String field, org.apache.lucene.search.Query luceneQuery) {
		if (luceneQuery == null) {
			return new TermQuery(field, "");
		}
		if (luceneQuery instanceof org.apache.lucene.search.TermQuery) {
			return new TermQuery(((org.apache.lucene.search.TermQuery) luceneQuery).getTerm());
		}
		if (luceneQuery instanceof org.apache.lucene.search.BooleanQuery) {
			var booleanQuery = (org.apache.lucene.search.BooleanQuery) luceneQuery;
			var queryParts = new ArrayList<BooleanQueryPart>();
			for (BooleanClause booleanClause : booleanQuery) {
				org.apache.lucene.search.Query queryPartQuery = booleanClause.getQuery();

				Occur occur;
				switch (booleanClause.getOccur()) {
					case MUST:
						occur = Occur.MUST;
						break;
					case FILTER:
						occur = Occur.FILTER;
						break;
					case SHOULD:
						occur = Occur.SHOULD;
						break;
					case MUST_NOT:
						occur = Occur.MUST_NOT;
						break;
					default:
						throw new IllegalArgumentException();
				}
				queryParts.add(new BooleanQueryPart(transformQuery(field, queryPartQuery), occur));
			}
			return new BooleanQuery(queryParts).setMinShouldMatch(booleanQuery.getMinimumNumberShouldMatch());
		}
		org.apache.lucene.search.SynonymQuery synonymQuery = (org.apache.lucene.search.SynonymQuery) luceneQuery;
		return new SynonymQuery(field,
				synonymQuery.getTerms().stream().map(TermQuery::new).toArray(TermQuery[]::new)
		);
	}

	private static List<TermPosition> getTerms(TextFieldsAnalyzer preferredAnalyzer, String field, String text) throws IOException {
		Analyzer analyzer = LuceneUtils.getAnalyzer(preferredAnalyzer);
		TokenStream ts = analyzer.tokenStream(field, new StringReader(text));
		return getTerms(ts, field);
	}

	private static List<TermPosition> getTerms(TokenStream ts, String field) throws IOException {
		TermToBytesRefAttribute charTermAttr = ts.addAttribute(TermToBytesRefAttribute.class);
		PositionIncrementAttribute positionIncrementTermAttr = ts.addAttribute(PositionIncrementAttribute.class);
		List<TermPosition> terms = new LinkedList<>();
		try (ts) {
			ts.reset(); // Resets this stream to the beginning. (Required)
			int termPosition = -1;
			while (ts.incrementToken()) {
				var tokenPositionIncrement = positionIncrementTermAttr.getPositionIncrement();
				termPosition += tokenPositionIncrement;
				terms.add(new TermPosition(new Term(field, charTermAttr.getBytesRef()), termPosition));
			}
			ts.end();   // Perform end-of-stream operations, e.g. set the final offset.
		}
		// Release resources associated with this stream.
		return terms;
	}
}
