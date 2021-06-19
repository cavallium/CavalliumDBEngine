package it.cavallium.dbengine.database;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.StringJoiner;

public record Column(String name) {

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
}
