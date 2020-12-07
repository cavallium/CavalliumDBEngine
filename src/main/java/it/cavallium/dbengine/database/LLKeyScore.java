package it.cavallium.dbengine.database;

import java.util.Objects;

public class LLKeyScore {

	private final String key;
	private final float score;

	public LLKeyScore(String key, float score) {
		this.key = key;
		this.score = score;
	}

	public String getKey() {
		return key;
	}

	public float getScore() {
		return score;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLKeyScore that = (LLKeyScore) o;
		return score == that.score &&
				Objects.equals(key, that.key);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, score);
	}

	@Override
	public String toString() {
		return "LLKeyScore{" +
				"key=" + key +
				", score=" + score +
				'}';
	}
}
