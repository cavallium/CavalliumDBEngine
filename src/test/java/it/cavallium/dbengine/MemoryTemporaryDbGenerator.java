package it.cavallium.dbengine;

import it.cavallium.dbengine.DbTestUtils.TempDb;
import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.memory.LLMemoryDatabaseConnection;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Mono;

public class MemoryTemporaryDbGenerator implements TemporaryDbGenerator {

	@Override
	public Mono<TempDb> openTempDb(TestAllocator allocator) {
		boolean canUseNettyDirect = DbTestUtils.computeCanUseNettyDirect();
		return Mono
				.fromCallable(() -> new LLMemoryDatabaseConnection(allocator.allocator()))
				.flatMap(conn -> conn
						.getDatabase("testdb",
								List.of(Column.dictionary("testmap"), Column.special("ints"), Column.special("longs")),
								new DatabaseOptions(Map.of(), true, false, true, false, true, canUseNettyDirect, canUseNettyDirect, -1)
						)
						.map(db -> new TempDb(allocator, conn, db, null)));
	}

	@Override
	public Mono<Void> closeTempDb(TempDb db) {
		return db.db().close();
	}
}
