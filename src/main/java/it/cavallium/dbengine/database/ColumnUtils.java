package it.cavallium.dbengine.database;

import it.cavallium.dbengine.rpc.current.data.Column;
import java.nio.charset.StandardCharsets;

public class ColumnUtils {

	private ColumnUtils() {

	}

	public static Column dictionary(String name) {
		return new Column("hash_map_" + name);
	}

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
