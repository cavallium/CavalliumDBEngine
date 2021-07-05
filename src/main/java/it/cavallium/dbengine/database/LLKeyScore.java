package it.cavallium.dbengine.database;

import java.util.Objects;
import java.util.StringJoiner;
import reactor.core.publisher.Mono;

public record LLKeyScore(int docId, float score, Mono<String> key) {

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLKeyScore that = (LLKeyScore) o;
		return docId == that.docId && Float.compare(that.score, score) == 0;
	}

	@Override
	public int hashCode() {
		return Objects.hash(docId, score);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", LLKeyScore.class.getSimpleName() + "[", "]")
				.add("docId=" + docId)
				.add("score=" + score)
				.toString();
	}
}
