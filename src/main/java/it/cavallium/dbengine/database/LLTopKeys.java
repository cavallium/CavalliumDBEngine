package it.cavallium.dbengine.database;

import java.util.Arrays;
import java.util.Objects;

public class LLTopKeys {

	private final long totalHitsCount;
	private final LLKeyScore[] hits;

	public LLTopKeys(long totalHitsCount, LLKeyScore[] hits) {
		this.totalHitsCount = totalHitsCount;
		this.hits = hits;
	}

	public long getTotalHitsCount() {
		return totalHitsCount;
	}

	public LLKeyScore[] getHits() {
		return hits;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLTopKeys llTopKeys = (LLTopKeys) o;
		return totalHitsCount == llTopKeys.totalHitsCount &&
				Arrays.equals(hits, llTopKeys.hits);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(totalHitsCount);
		result = 31 * result + Arrays.hashCode(hits);
		return result;
	}

	@Override
	public String toString() {
		return "LLTopKeys{" +
				"totalHitsCount=" + totalHitsCount +
				", hits=" + Arrays.toString(hits) +
				'}';
	}
}
