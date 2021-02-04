package it.cavallium.dbengine.database;

import java.util.Objects;

public class LLSort {

	private final String fieldName;
	private final LLSortType type;
	private final boolean reverse;

	public LLSort(String fieldName, LLSortType type, boolean reverse) {
		this.fieldName = fieldName;
		this.type = type;
		this.reverse = reverse;
	}

	public static LLSort newSortedNumericSortField(String fieldName, boolean reverse) {
		return new LLSort(fieldName, LLSortType.LONG, reverse);
	}

	public static LLSort newRandomSortField() {
		return new LLSort(null, LLSortType.RANDOM, false);
	}

	public static LLSort newSortScore() {
		return new LLSort(null, LLSortType.SCORE, false);
	}

	public static LLSort newSortDoc() {
		return new LLSort(null, LLSortType.DOC, false);
	}

	public String getFieldName() {
		return fieldName;
	}

	public LLSortType getType() {
		return type;
	}

	public boolean isReverse() {
		return reverse;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLSort llSort = (LLSort) o;
		return reverse == llSort.reverse &&
				Objects.equals(fieldName, llSort.fieldName) &&
				type == llSort.type;
	}

	@Override
	public int hashCode() {
		return Objects.hash(fieldName, type, reverse);
	}

	@Override
	public String toString() {
		return "LLSort{" +
				"fieldName='" + fieldName + '\'' +
				", type=" + type +
				", reverse=" + reverse +
				'}';
	}
}
