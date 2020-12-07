package it.cavallium.luceneserializer.luceneserializer;

public class MatchAllDocsQuery implements Query {

	public MatchAllDocsQuery() {
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringifyUtils.writeHeader(output, QueryConstructorType.MATCH_ALL_DOCS_QUERY, data);
	}
}
