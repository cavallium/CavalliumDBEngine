package it.cavallium.dbengine.lucene.serializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class BooleanQuery implements Query {

	private final BooleanQueryPart[] parts;
	private int minShouldMatch;

	public BooleanQuery(BooleanQueryPart... parts) {
		this.parts = parts;
	}

	public BooleanQuery(Collection<BooleanQueryPart> parts) {
		this.parts = parts.toArray(BooleanQueryPart[]::new);
	}

	public BooleanQuery setMinShouldMatch(int minShouldMatch) {
		this.minShouldMatch = minShouldMatch;
		return this;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringifyUtils.stringifyInt(data, minShouldMatch);
		StringBuilder listData = new StringBuilder();
		listData.append(parts.length).append('|');
		for (BooleanQueryPart part : parts) {
			part.stringify(listData);
		}
		StringifyUtils.writeHeader(data, QueryConstructorType.BOOLEAN_QUERY_INFO_LIST, listData);
		StringifyUtils.writeHeader(output, QueryConstructorType.BOOLEAN_QUERY, data);
	}

	@Override
	public String toString() {
		return Arrays.stream(parts).map(Object::toString).collect(Collectors.joining(" || ", "((", ") Minimum should matches:" + minShouldMatch + ")"));
	}
}
