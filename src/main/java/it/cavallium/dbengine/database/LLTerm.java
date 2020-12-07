package it.cavallium.dbengine.database;

import java.util.Objects;

public class LLTerm {

	private final String key;
	private final String value;

	public LLTerm(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "LLTerm{" +
				"key='" + key + '\'' +
				", value='" + value + '\'' +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LLTerm llTerm = (LLTerm) o;
		return Objects.equals(key, llTerm.key) &&
				Objects.equals(value, llTerm.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, value);
	}
}
