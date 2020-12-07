package it.cavallium.luceneserializer.luceneserializer;

public class BoostQuery implements Query {

	private final Query query;
	private final float boostValue;

	public BoostQuery(Query query, float boostValue) {
		this.query = query;
		this.boostValue = boostValue;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		query.stringify(data);
		StringifyUtils.stringifyFloat(data, boostValue);
		StringifyUtils.writeHeader(output, QueryConstructorType.BOOST_QUERY, data);
	}
}
