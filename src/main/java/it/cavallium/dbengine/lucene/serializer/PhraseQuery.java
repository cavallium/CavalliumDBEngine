package it.cavallium.dbengine.lucene.serializer;

public class PhraseQuery implements Query {

	// some terms can be null
	private final TermPosition[] parts;

	public PhraseQuery(TermPosition... parts) {
		this.parts = parts;
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
		StringifyUtils.writeHeader(output, QueryConstructorType.PHRASE_QUERY, data);
	}
}
