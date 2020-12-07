package it.cavallium.luceneserializer.luceneserializer;

public class SortedNumericDocValuesFieldSlowRangeQuery implements Query {

	private final String name;
	private final long min;
	private final long max;

	public SortedNumericDocValuesFieldSlowRangeQuery(String name, long min, long max) {
		this.name = name;
		this.min = min;
		this.max = max;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringifyUtils.stringifyString(data, name);
		StringifyUtils.stringifyLong(data, min);
		StringifyUtils.stringifyLong(data, max);
		StringifyUtils.writeHeader(output, QueryConstructorType.SORTED_SLOW_RANGE_QUERY, data);
	}
}
