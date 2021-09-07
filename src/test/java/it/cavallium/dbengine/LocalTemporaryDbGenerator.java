package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;

import it.cavallium.dbengine.DbTestUtils.TempDb;
import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class LocalTemporaryDbGenerator implements TemporaryDbGenerator {

	private static final AtomicInteger dbId = new AtomicInteger(0);

	@Override
	public Mono<TempDb> openTempDb(TestAllocator allocator) {
		boolean canUseNettyDirect = DbTestUtils.computeCanUseNettyDirect();
		return Mono.defer(() -> {
			var wrkspcPath = Path.of("/tmp/.cache/tempdb-" + dbId.incrementAndGet() + "/");
			return Mono
					.<LLKeyValueDatabase>fromCallable(() -> {
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
						return null;
					})
					.subscribeOn(Schedulers.boundedElastic())
					.then(new LLLocalDatabaseConnection(allocator.allocator(), wrkspcPath).connect())
					.flatMap(conn -> conn
							.getDatabase("testdb",
									List.of(Column.dictionary("testmap"), Column.special("ints"), Column.special("longs")),
									new DatabaseOptions(Map.of(), true, false, true, false, true, canUseNettyDirect, canUseNettyDirect, -1)
							)
							.map(db -> new TempDb(allocator, conn, db, wrkspcPath))
					);
		});
	}

	@Override
	public Mono<Void> closeTempDb(TempDb tempDb) {
		return tempDb.db().close().then(tempDb.connection().disconnect()).then(Mono.fromCallable(() -> {
			ensureNoLeaks(tempDb.allocator().allocator(), false, false);
			if (Files.exists(tempDb.path())) {
				Files.walk(tempDb.path()).sorted(Comparator.reverseOrder()).forEach(file -> {
					try {
						Files.delete(file);
					} catch (IOException ex) {
						throw new CompletionException(ex);
					}
				});
			}
			return null;
		}).subscribeOn(Schedulers.boundedElastic())).then();
	}
}
