package it.cavallium.dbengine.lucene.serializer;

import it.cavallium.dbengine.database.LLTerm;
import org.apache.lucene.index.Term;

public class WildcardQuery implements Query {

	private final Term term;

	public WildcardQuery(Term term) {
		this.term = term;
	}

	public WildcardQuery(LLTerm term) {
		this.term = new Term(term.getKey(), term.getValue());
	}

	public WildcardQuery(String name, String val) {
		this.term = new Term(name, val);
	}

	public Term getTerm() {
		return term;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringifyUtils.stringifyTerm(data, term);
		StringifyUtils.writeHeader(output, QueryConstructorType.WILDCARD_QUERY, data);
	}

	@Override
	public String toString() {
		return "(" + term.field() + ":" + term.text() + ")";
	}
}
