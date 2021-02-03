package it.cavallium.dbengine.lucene.serializer;

import java.util.Collection;

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
}
