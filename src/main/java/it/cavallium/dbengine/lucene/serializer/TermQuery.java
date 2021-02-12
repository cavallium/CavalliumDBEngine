package it.cavallium.dbengine.lucene.serializer;

import org.apache.lucene.index.Term;

public class TermQuery implements Query {

	private final Term term;

	public TermQuery(Term term) {
		this.term = term;
	}

	public TermQuery(String name, String val) {
		this.term = new Term(name, val);
	}

	public Term getTerm() {
		return term;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringifyUtils.stringifyTerm(data, term);
		StringifyUtils.writeHeader(output, QueryConstructorType.TERM_QUERY, data);
	}

	@Override
	public String toString() {
		return "(" + term.field() + ":" + term.text() + ")";
	}
}
