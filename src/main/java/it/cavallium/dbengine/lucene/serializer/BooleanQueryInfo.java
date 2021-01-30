package it.cavallium.dbengine.lucene.serializer;

import java.util.Objects;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

public class BooleanQueryInfo {

	public final Query query;
	public final BooleanClause.Occur occur;

	public BooleanQueryInfo(Query query, BooleanClause.Occur occur) {
		this.query = query;
		this.occur = occur;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BooleanQueryInfo that = (BooleanQueryInfo) o;
		return Objects.equals(query, that.query) &&
				occur == that.occur;
	}

	@Override
	public int hashCode() {
		return Objects.hash(query, occur);
	}

	@Override
	public String toString() {
		return "BooleanQueryInfo{" +
				"query=" + query +
				", occur=" + occur +
				'}';
	}
}
