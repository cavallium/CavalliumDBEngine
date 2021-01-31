package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import java.nio.file.Path;
import java.util.List;

public class Example {

	public static void main(String[] args) {
		System.out.println("Test");
		new LLLocalDatabaseConnection(Path.of("/tmp/"), true)
				.connect()
				.flatMap(conn -> conn.getDatabase("testdb", List.of(Column.hashMap("testmap")), false))
				.block();
	}
}