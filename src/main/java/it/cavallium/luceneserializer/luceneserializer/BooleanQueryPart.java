package it.cavallium.luceneserializer.luceneserializer;

public class BooleanQueryPart implements SerializedQueryObject {

	private final Query query;
	private final Occur occur;

	public BooleanQueryPart(Query query, Occur occur) {
		this.query = query;
		this.occur = occur;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		query.stringify(data);
		occur.stringify(data);
		StringifyUtils.writeHeader(output, QueryConstructorType.BOOLEAN_QUERY_INFO, data);
	}
}
