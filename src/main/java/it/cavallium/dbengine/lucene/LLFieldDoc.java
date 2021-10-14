package it.cavallium.dbengine.lucene;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public record LLFieldDoc(int doc, float score, int shardIndex, List<Object> fields) implements LLDocElement {

	@Override
	public String toString() {
		return "doc=" + doc + " score=" + score + " shardIndex=" + shardIndex + " fields="+ fields.stream()
				.map(Objects::toString).collect(Collectors.joining(",", "[", "]"));
	}
}
