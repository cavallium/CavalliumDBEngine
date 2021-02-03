package it.cavallium.dbengine.lucene.serializer;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.automaton.LevenshteinAutomata;

@SuppressWarnings("unused")
public class FuzzyQuery implements Query {

	private final Term term;
	private final int val1;
	private final int val2;
	private final int val3;
	private final boolean bool;

	/**
	 * Create a new FuzzyQuery that will match terms with an edit distance of at most
	 * <code>maxEdits</code> to <code>term</code>. If a <code>prefixLength</code> &gt; 0 is
	 * specified, a common prefix of that length is also required.
	 *
	 * @param term           the term to search for
	 * @param maxEdits       must be {@code >= 0} and {@code <=} {@link LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE}.
	 * @param prefixLength   length of common (non-fuzzy) prefix
	 * @param maxExpansions  the maximum number of terms to match. If this number is greater than
	 *                       {@link BooleanQuery#getMaxClauseCount} when the query is rewritten, then
	 *                       the maxClauseCount will be used instead.
	 * @param transpositions true if transpositions should be treated as a primitive edit operation.
	 *                       If this is false, comparisons will implement the classic Levenshtein
	 *                       algorithm.
	 */
	public FuzzyQuery(Term term, int maxEdits, int prefixLength, int maxExpansions,
			boolean transpositions) {
		this.term = term;
		this.val1 = maxEdits;
		this.val2 = prefixLength;
		this.val3 = maxExpansions;
		this.bool = transpositions;
	}

	@Override
	public void stringify(StringBuilder output) {
		StringBuilder data = new StringBuilder();
		StringifyUtils.stringifyTerm(data, term);
		StringifyUtils.stringifyInt(data, val1);
		StringifyUtils.stringifyInt(data, val2);
		StringifyUtils.stringifyInt(data, val3);
		StringifyUtils.stringifyBool(data, bool);
		StringifyUtils.writeHeader(output, QueryConstructorType.FUZZY_QUERY, data);
	}
}
