package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.MAX_IN_MEMORY_RESULT_ENTRIES;
import static it.cavallium.dbengine.DbTestUtils.ensureNoLeaks;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.data.generator.nativedata.Nullableboolean;
import it.cavallium.data.generator.nativedata.Nullabledouble;
import it.cavallium.data.generator.nativedata.Nullableint;
import it.cavallium.data.generator.nativedata.Nullablelong;
import it.cavallium.dbengine.DbTestUtils.TempDb;
import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.database.ColumnUtils;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.dbengine.rpc.current.data.ByteBuffersDirectory;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
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
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

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
			true,
			MAX_IN_MEMORY_RESULT_ENTRIES
	);

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
					.then(new LLLocalDatabaseConnection(allocator.allocator(),
							new SimpleMeterRegistry(),
							wrkspcPath,
							true,
							null
					).connect())
					.flatMap(conn -> {
						SwappableLuceneSearcher searcher = new SwappableLuceneSearcher();
						var luceneHacks = new LuceneHacks(() -> searcher, () -> searcher);
						return Mono.zip(
										conn.getDatabase("testdb",
												List.of(ColumnUtils.dictionary("testmap"), ColumnUtils.special("ints"), ColumnUtils.special("longs")),
												new DatabaseOptions(List.of(),
														Map.of(),
														true,
														false,
														false,
														true,
														canUseNettyDirect,
														false,
														Nullableint.of(-1),
														Nullablelong.empty(),
														Nullablelong.empty(),
														Nullableboolean.empty()
												)
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
										Mono.just(searcher)
								)
								.map(tuple -> new TempDb(allocator, conn, tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), wrkspcPath));
					});
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
