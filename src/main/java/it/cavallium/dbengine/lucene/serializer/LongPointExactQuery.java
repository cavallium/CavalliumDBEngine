package it.cavallium.dbengine.lucene.serializer;

public class LongPointExactQuery implements Query {

	private final String name;
	private final long value;

	public LongPointExactQuery(String name, long value) {
		this.name = name;
		this.value = value;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringifyUtils.stringifyString(data, name);
		StringifyUtils.stringifyLong(data, value);
		StringifyUtils.writeHeader(output, QueryConstructorType.LONG_POINT_EXACT_QUERY, data);
	}
}
