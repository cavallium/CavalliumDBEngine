package it.cavallium.luceneserializer.luceneserializer;

import org.apache.lucene.index.Term;

public class TermPosition implements Query {

	private final Term term;
	private final int position;

	public TermPosition(Term term, int position) {
		this.term = term;
		this.position = position;
	}

	public Term getTerm() {
		return term;
	}

	public int getPosition() {
		return position;
	}

	@Override
	public void stringify(StringBuilder output) {

	}
}
