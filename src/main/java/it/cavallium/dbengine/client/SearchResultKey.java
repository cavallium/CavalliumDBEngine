package it.cavallium.dbengine.client;

import java.util.Objects;
import java.util.StringJoiner;

public class SearchResultKey<T> {
	private final T key;
	private final float score;

	public SearchResultKey(T key, float score) {
		this.key = key;
		this.score = score;
	}

	public float getScore() {
		return score;
	}

	public T getKey() {
		return key;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SearchResultKey<?> that = (SearchResultKey<?>) o;
		return Float.compare(that.score, score) == 0 && Objects.equals(key, that.key);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, score);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", SearchResultKey.class.getSimpleName() + "[", "]")
				.add("key=" + key)
				.add("score=" + score)
				.toString();
	}
}
