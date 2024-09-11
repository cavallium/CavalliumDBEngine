package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.ensureNoLeaks;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.dbengine.tests.DbTestUtils.TempDb;
import it.cavallium.dbengine.client.DefaultDatabaseOptions;
import it.cavallium.dbengine.database.ColumnUtils;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalTemporaryDbGenerator implements TemporaryDbGenerator {

	private static final AtomicInteger dbId = new AtomicInteger(0);


	@Override
	public TempDb openTempDb() throws IOException {
		var wrkspcPath = Path.of("/tmp/.cache/tempdb-" + dbId.incrementAndGet() + "/");

		if (Files.exists(wrkspcPath)) {
			Files.walk(wrkspcPath).sorted(Comparator.reverseOrder()).forEach(file -> {
				try {
					Files.delete(file);
				} catch (IOException ex) {
					throw new CompletionException(ex);
				}
			});
		}
		Files.createDirectories(wrkspcPath);

		LLDatabaseConnection conn = new LLLocalDatabaseConnection(
				new SimpleMeterRegistry(),
				wrkspcPath,
				true
		).connect();

		return new TempDb(conn,
				conn.getDatabase("testdb",
						List.of(ColumnUtils.dictionary("testmap"), ColumnUtils.special("ints"), ColumnUtils.special("longs")),
						DefaultDatabaseOptions.builder().build()
				),
				wrkspcPath
		);
	}

	@Override
	public void closeTempDb(TempDb tempDb) throws IOException {
		tempDb.db().close();
		tempDb.connection().disconnect();
		ensureNoLeaks();
		if (Files.exists(tempDb.path())) {
			try (var walk = Files.walk(tempDb.path())) {
				walk.sorted(Comparator.reverseOrder()).forEach(file -> {
					try {
						Files.delete(file);
					} catch (IOException ex) {
						throw new CompletionException(ex);
					}
				});
			}
		}
	}
}
