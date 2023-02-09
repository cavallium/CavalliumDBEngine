package it.cavallium.dbengine.database;

import java.util.Objects;
import org.apache.lucene.util.BytesRef;

public class LLTerm {

	private final String key;
	private final BytesRef value;

	public LLTerm(String key, String value) {
		this.key = key;
		this.value = new BytesRef(value);
	}

	public LLTerm(String key, BytesRef value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public String getValueUTF8() {
		return value.utf8ToString();
	}

	public BytesRef getValueBytesRef() {
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
