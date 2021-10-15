package it.cavallium.dbengine.lucene;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.search.FieldDoc;

public record LLFieldDoc(int doc, float score, int shardIndex, List<Object> fields) implements LLDoc {

	@Override
	public String toString() {
		return "doc=" + doc + " score=" + score + " shardIndex=" + shardIndex + " fields="+ fields.stream()
				.map(Objects::toString).collect(Collectors.joining(",", "[", "]"));
	}

	public FieldDoc toFieldDoc() {
		return new FieldDoc(doc, score, fields.toArray(Object[]::new), shardIndex);
	}
}
