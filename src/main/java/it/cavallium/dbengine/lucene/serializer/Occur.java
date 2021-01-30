package it.cavallium.dbengine.lucene.serializer;

import org.apache.lucene.search.BooleanClause;

public class Occur implements SerializedQueryObject {

	private final BooleanClause.Occur occur;

	public Occur(BooleanClause.Occur occur) {
		this.occur = occur;
	}

	public static Occur MUST = new Occur(BooleanClause.Occur.MUST);
	public static Occur FILTER = new Occur(BooleanClause.Occur.FILTER);
	public static Occur SHOULD = new Occur(BooleanClause.Occur.SHOULD);
	public static Occur MUST_NOT = new Occur(BooleanClause.Occur.MUST_NOT);

	@Override
	public void stringify(StringBuilder output) {
		switch (occur) {
			case MUST:
				StringifyUtils.writeHeader(output, QueryConstructorType.OCCUR_MUST, new StringBuilder());
				break;
			case FILTER:
				StringifyUtils.writeHeader(output, QueryConstructorType.OCCUR_FILTER, new StringBuilder());
				break;
			case SHOULD:
				StringifyUtils.writeHeader(output, QueryConstructorType.OCCUR_SHOULD, new StringBuilder());
				break;
			case MUST_NOT:
				StringifyUtils.writeHeader(output, QueryConstructorType.OCCUR_MUST_NOT, new StringBuilder());
				break;
		}
	}
}
