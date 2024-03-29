package it.cavallium.dbengine.client.query;

import static it.cavallium.dbengine.database.LLUtils.mapList;

import it.cavallium.dbengine.client.query.current.data.BooleanQuery;
import it.cavallium.dbengine.client.query.current.data.BooleanQueryPart;
import it.cavallium.dbengine.client.query.current.data.Occur;
import it.cavallium.dbengine.client.query.current.data.OccurFilter;
import it.cavallium.dbengine.client.query.current.data.OccurMust;
import it.cavallium.dbengine.client.query.current.data.OccurMustNot;
import it.cavallium.dbengine.client.query.current.data.OccurShould;
import it.cavallium.dbengine.client.query.current.data.PhraseQuery;
import it.cavallium.dbengine.client.query.current.data.Query;
import it.cavallium.dbengine.client.query.current.data.SynonymQuery;
import it.cavallium.dbengine.client.query.current.data.TermAndBoost;
import it.cavallium.dbengine.client.query.current.data.TermPosition;
import it.cavallium.dbengine.client.query.current.data.TermQuery;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.util.QueryBuilder;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class QueryUtils {

	/**
	 * @param fraction of query terms [0..1] that should match
	 */
	public static Query sparseWordsSearch(TextFieldsAnalyzer preferredAnalyzer,
			String field,
			String text,
			float fraction) {
		var qb = new QueryBuilder(LuceneUtils.getAnalyzer(preferredAnalyzer));
		var luceneQuery = qb.createMinShouldMatchQuery(field, text, fraction);
		return transformQuery(field, luceneQuery);
	}

	/**
	 * Deprecated: use solr SolrTextQuery
	 */
	@Deprecated
	public static Query phraseSearch(TextFieldsAnalyzer preferredAnalyzer, String field, String text, int slop) {
		var qb = new QueryBuilder(LuceneUtils.getAnalyzer(preferredAnalyzer));
		var luceneQuery = qb.createPhraseQuery(field, text, slop);
		return transformQuery(field, luceneQuery);
	}

	/**
	 * Deprecated: use solr SolrTextQuery
	 */
	public static Query exactSearch(TextFieldsAnalyzer preferredAnalyzer, String field, String text) {
		var qb = new QueryBuilder(LuceneUtils.getAnalyzer(preferredAnalyzer));
		var luceneQuery = qb.createPhraseQuery(field, text);
		return transformQuery(field, luceneQuery);
	}

	@NotNull
	private static Query transformQuery(String field, org.apache.lucene.search.Query luceneQuery) {
		if (luceneQuery == null) {
			return TermQuery.of(it.cavallium.dbengine.client.query.current.data.Term.of(field, ""));
		}
		if (luceneQuery instanceof org.apache.lucene.search.TermQuery) {
			return TermQuery.of(QueryParser.toQueryTerm(((org.apache.lucene.search.TermQuery) luceneQuery).getTerm()));
		}
		if (luceneQuery instanceof org.apache.lucene.search.BooleanQuery) {
			var booleanQuery = (org.apache.lucene.search.BooleanQuery) luceneQuery;
			var queryParts = new ArrayList<BooleanQueryPart>();
			for (BooleanClause booleanClause : booleanQuery) {
				org.apache.lucene.search.Query queryPartQuery = booleanClause.getQuery();

				Occur occur = switch (booleanClause.getOccur()) {
					case MUST -> OccurMust.of();
					case FILTER -> OccurFilter.of();
					case SHOULD -> OccurShould.of();
					case MUST_NOT -> OccurMustNot.of();
				};
				queryParts.add(BooleanQueryPart.of(transformQuery(field, queryPartQuery), occur));
			}
			return BooleanQuery.of(List.copyOf(queryParts), booleanQuery.getMinimumNumberShouldMatch());
		}
		if (luceneQuery instanceof org.apache.lucene.search.PhraseQuery phraseQuery) {
			int slop = phraseQuery.getSlop();
			var terms = phraseQuery.getTerms();
			var positions = phraseQuery.getPositions();
			TermPosition[] termPositions = new TermPosition[terms.length];
			for (int i = 0; i < terms.length; i++) {
				var term = terms[i];
				var position = positions[i];
				termPositions[i] = TermPosition.of(QueryParser.toQueryTerm(term), position);
			}
			return PhraseQuery.of(List.of(termPositions), slop);
		}
		org.apache.lucene.search.SynonymQuery synonymQuery = (org.apache.lucene.search.SynonymQuery) luceneQuery;
		return SynonymQuery.of(field,
				mapList(synonymQuery.getTerms(), term -> TermAndBoost.of(QueryParser.toQueryTerm(term), 1))
		);
	}
}
