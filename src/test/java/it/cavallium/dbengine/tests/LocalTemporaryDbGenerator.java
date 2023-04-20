package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.tests.DbTestUtils.MAX_IN_MEMORY_RESULT_ENTRIES;
import static it.cavallium.dbengine.tests.DbTestUtils.ensureNoLeaks;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.datagen.nativedata.Nullableboolean;
import it.cavallium.datagen.nativedata.Nullabledouble;
import it.cavallium.datagen.nativedata.Nullableint;
import it.cavallium.dbengine.tests.DbTestUtils.TempDb;
import it.cavallium.dbengine.client.DefaultDatabaseOptions;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.database.ColumnUtils;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.dbengine.rpc.current.data.ByteBuffersDirectory;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalTemporaryDbGenerator implements TemporaryDbGenerator {

	private static final AtomicInteger dbId = new AtomicInteger(0);

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
				wrkspcPath
		);
	}

	@Override
	public void closeTempDb(TempDb tempDb) throws IOException {
		tempDb.db().close();
		tempDb.connection().disconnect();
		tempDb.swappableLuceneSearcher().close();
		tempDb.luceneMulti().close();
		tempDb.luceneSingle().close();
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
