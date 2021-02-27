package it.cavallium.dbengine.lucene.serializer;

public class ConstantScoreQuery implements Query {

	private final Query query;

	public ConstantScoreQuery(Query query) {
		this.query = query;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		query.stringify(data);
		StringifyUtils.writeHeader(output, QueryConstructorType.CONSTANT_SCORE_QUERY, data);
	}

	@Override
	public String toString() {
		return "(" + query + " *1)";
	}
}
