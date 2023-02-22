package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.MAX_IN_MEMORY_RESULT_ENTRIES;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.data.generator.nativedata.Nullableboolean;
import it.cavallium.data.generator.nativedata.Nullabledouble;
import it.cavallium.data.generator.nativedata.Nullableint;
import it.cavallium.dbengine.tests.DbTestUtils.TempDb;
import it.cavallium.dbengine.client.DefaultDatabaseOptions;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.database.ColumnUtils;
import it.cavallium.dbengine.database.memory.LLMemoryDatabaseConnection;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.dbengine.rpc.current.data.ByteBuffersDirectory;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class MemoryTemporaryDbGenerator implements TemporaryDbGenerator {

	private static final LuceneOptions LUCENE_OPTS = new LuceneOptions(Map.of(),
			Duration.ofSeconds(5),
			Duration.ofSeconds(5),
			false,
			new ByteBuffersDirectory(),
			Nullableboolean.empty(),
			Nullabledouble.empty(),
			Nullableint.empty(),
			Nullableboolean.empty(),
			Nullableboolean.empty(),
			MAX_IN_MEMORY_RESULT_ENTRIES,
			LuceneUtils.getDefaultMergePolicy()
	);

	@Override
	public TempDb openTempDb() {
		var conn = new LLMemoryDatabaseConnection(new SimpleMeterRegistry());

		SwappableLuceneSearcher searcher = new SwappableLuceneSearcher();
		var luceneHacks = new LuceneHacks(() -> searcher, () -> searcher);
		return new TempDb(conn,
				conn.getDatabase("testdb",
						List.of(ColumnUtils.dictionary("testmap"), ColumnUtils.special("ints"), ColumnUtils.special("longs")),
						DefaultDatabaseOptions.builder().build()
				),
				conn.getLuceneIndex("testluceneindex1",
						LuceneUtils.singleStructure(),
						IndicizerAnalyzers.of(TextFieldsAnalyzer.ICUCollationKey),
						IndicizerSimilarities.of(TextFieldsSimilarity.Boolean),
						LUCENE_OPTS,
						luceneHacks
				),
				conn.getLuceneIndex("testluceneindex16",
						LuceneUtils.shardsStructure(3),
						IndicizerAnalyzers.of(TextFieldsAnalyzer.ICUCollationKey),
						IndicizerSimilarities.of(TextFieldsSimilarity.Boolean),
						LUCENE_OPTS,
						luceneHacks
				),
				searcher,
				null
		);
	}

	@Override
	public void closeTempDb(TempDb db) {
		db.db().close();
	}
}
