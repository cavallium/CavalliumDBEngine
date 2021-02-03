package it.cavallium.dbengine.client;

import java.util.Objects;
import java.util.StringJoiner;

public class SearchResultItem<T, U> {
	private final T key;
	private final U value;
	private final float score;

	public SearchResultItem(T key, U value, float score) {
		this.key = key;
		this.value = value;
		this.score = score;
	}

	public float getScore() {
		return score;
	}

	public T getKey() {
		return key;
	}

	public U getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SearchResultItem<?, ?> that = (SearchResultItem<?, ?>) o;
		return Float.compare(that.score, score) == 0 && Objects.equals(key, that.key) && Objects.equals(value, that.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, value, score);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", SearchResultItem.class.getSimpleName() + "[", "]")
				.add("key=" + key)
				.add("value=" + value)
				.add("score=" + score)
				.toString();
	}
}
