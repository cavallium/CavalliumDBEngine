package it.cavallium.dbengine.lucene.serializer;

import java.util.Arrays;
import java.util.stream.Collectors;

public class PhraseQuery implements Query {

	// some terms can be null
	private final TermPosition[] parts;
	private int slop;

	public PhraseQuery(TermPosition... parts) {
		this.parts = parts;
		this.slop = 0;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringBuilder listData = new StringBuilder();
		listData.append(parts.length).append('|');
		for (TermPosition part : parts) {
			StringifyUtils.stringifyTermPosition(listData, part);
		}
		StringifyUtils.writeHeader(data, QueryConstructorType.TERM_POSITION_LIST, listData);
		StringifyUtils.stringifyInt(data, slop);
		StringifyUtils.writeHeader(output, QueryConstructorType.PHRASE_QUERY, data);
	}

	@Override
	public String toString() {
		return Arrays.stream(parts).map(Object::toString).collect(Collectors.joining(", ", "(", ")"));
	}

	public PhraseQuery setSlop(int slop) {
		this.slop = slop;
		return this;
	}
}
