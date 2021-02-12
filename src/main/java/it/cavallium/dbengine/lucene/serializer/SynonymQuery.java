package it.cavallium.dbengine.lucene.serializer;

import java.util.Arrays;
import java.util.stream.Collectors;

public class SynonymQuery implements Query {

	private final String field;
	// some terms can be null
	private final TermQuery[] parts;

	public SynonymQuery(String field, TermQuery... parts) {
		this.field = field;
		this.parts = parts;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringifyUtils.stringifyString(data, field);
		StringBuilder listData = new StringBuilder();
		listData.append(parts.length).append('|');
		for (TermQuery part : parts) {
			StringifyUtils.stringifyTermQuery(listData, part);
		}
		StringifyUtils.writeHeader(data, QueryConstructorType.TERM_QUERY_LIST, listData);
		StringifyUtils.writeHeader(output, QueryConstructorType.SYNONYM_QUERY, data);
	}

	@Override
	public String toString() {
		return Arrays.stream(parts).map(Object::toString).collect(Collectors.joining(", ", "(", ")"));
	}
}
