package it.cavallium.dbengine.tests;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.dbengine.tests.DbTestUtils.TempDb;
import it.cavallium.dbengine.client.DefaultDatabaseOptions;
import it.cavallium.dbengine.database.ColumnUtils;
import it.cavallium.dbengine.database.memory.LLMemoryDatabaseConnection;
import java.io.IOException;
import java.util.List;

public class MemoryTemporaryDbGenerator implements TemporaryDbGenerator {

	@Override
	public TempDb openTempDb() {
		var conn = new LLMemoryDatabaseConnection(new SimpleMeterRegistry());

		return new TempDb(conn,
				conn.getDatabase("testdb",
						List.of(ColumnUtils.dictionary("testmap"), ColumnUtils.special("ints"), ColumnUtils.special("longs")),
						DefaultDatabaseOptions.builder().build()
				),
				null
		);
	}

	@Override
	public void closeTempDb(TempDb tempDb) throws IOException {
		tempDb.db().close();
		tempDb.connection().disconnect();
	}
}
