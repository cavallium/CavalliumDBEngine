package it.cavallium.dbengine;

import static it.cavallium.dbengine.DbTestUtils.MAX_IN_MEMORY_RESULT_ENTRIES;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.dbengine.DbTestUtils.TempDb;
import it.cavallium.dbengine.DbTestUtils.TestAllocator;
import it.cavallium.dbengine.client.DatabaseOptions;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.LuceneOptions;
import it.cavallium.dbengine.client.NRTCachingOptions;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.memory.LLMemoryDatabaseConnection;
import it.cavallium.dbengine.lucene.LuceneHacks;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import reactor.core.publisher.Mono;

public class MemoryTemporaryDbGenerator implements TemporaryDbGenerator {

	private static final Optional<NRTCachingOptions> NRT = Optional.empty();
	private static final LuceneOptions LUCENE_OPTS = new LuceneOptions(Map.of(), Duration.ofSeconds(5), Duration.ofSeconds(5),
			false,
			true,
			Optional.empty(),
			true,
			NRT,
			16 * 1024 * 1024,
			true,
			false,
			false,
			MAX_IN_MEMORY_RESULT_ENTRIES
	);

	@Override
	public Mono<TempDb> openTempDb(TestAllocator allocator) {
		boolean canUseNettyDirect = DbTestUtils.computeCanUseNettyDirect();
		return Mono
				.fromCallable(() -> new LLMemoryDatabaseConnection(allocator.allocator(), new SimpleMeterRegistry()))
				.flatMap(conn -> {
					SwappableLuceneSearcher searcher = new SwappableLuceneSearcher();
					var luceneHacks = new LuceneHacks(() -> searcher, () -> searcher);
					return Mono
							.zip(
									conn.getDatabase("testdb",
											List.of(Column.dictionary("testmap"), Column.special("ints"), Column.special("longs")),
											new DatabaseOptions(List.of(), Map.of(), true, false, true, false, true, canUseNettyDirect, true, -1, null)
									),
									conn.getLuceneIndex(null,
											"testluceneindex1",
											1,
											IndicizerAnalyzers.of(TextFieldsAnalyzer.WordSimple),
											IndicizerSimilarities.of(TextFieldsSimilarity.Boolean),
											LUCENE_OPTS,
											luceneHacks
									),
									conn.getLuceneIndex("testluceneindex16",
											null,
											3,
											IndicizerAnalyzers.of(TextFieldsAnalyzer.WordSimple),
											IndicizerSimilarities.of(TextFieldsSimilarity.Boolean),
											LUCENE_OPTS,
											luceneHacks
									),
									Mono.just(searcher)
							)
							.map(tuple -> new TempDb(allocator, conn, tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), null));
				});
	}

	@Override
	public Mono<Void> closeTempDb(TempDb db) {
		return db.db().close();
	}
}
