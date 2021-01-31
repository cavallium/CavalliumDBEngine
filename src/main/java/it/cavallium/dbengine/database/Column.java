package it.cavallium.dbengine.database;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.StringJoiner;

public class Column {

	private final String name;

	private Column(String name) {
		this.name = name;
	}

	public static Column dictionary(String name) {
		return new Column("hash_map_" + name);
	}

	@Deprecated
	public static Column deprecatedSet(String name) {
		return new Column("hash_set_" + name);
	}

	public static Column special(String name) {
		return new Column(name);
	}

	public static String toString(byte[] name) {
		return new String(name, StandardCharsets.US_ASCII);
	}

	public String getName() {
		return name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Column)) {
			return false;
		}
		Column column = (Column) o;
		return Objects.equals(name, column.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", Column.class.getSimpleName() + "[", "]")
				.add("name='" + name + "'")
				.toString();
	}
}
