package it.cavallium.luceneserializer.luceneserializer;

public class IntPointExactQuery implements Query {

	private final String name;
	private final int value;

	public IntPointExactQuery(String name, int value) {
		this.name = name;
		this.value = value;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringifyUtils.stringifyString(data, name);
		StringifyUtils.stringifyInt(data, value);
		StringifyUtils.writeHeader(output, QueryConstructorType.INT_POINT_EXACT_QUERY, data);
	}
}
