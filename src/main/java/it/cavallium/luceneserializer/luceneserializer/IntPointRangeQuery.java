package it.cavallium.luceneserializer.luceneserializer;

public class IntPointRangeQuery implements Query {

	private final String name;
	private final int min;
	private final int max;

	public IntPointRangeQuery(String name, int min, int max) {
		this.name = name;
		this.min = min;
		this.max = max;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringifyUtils.stringifyString(data, name);
		StringifyUtils.stringifyInt(data, min);
		StringifyUtils.stringifyInt(data, max);
		StringifyUtils.writeHeader(output, QueryConstructorType.INT_POINT_RANGE_QUERY, data);
	}
}
