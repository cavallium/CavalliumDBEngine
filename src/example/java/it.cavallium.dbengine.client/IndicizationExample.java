package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLItem;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.lucene.serializer.TermQuery;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.StringJoiner;
import java.util.concurrent.CompletionException;
import org.apache.lucene.document.Field.Store;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class IndicizationExample {

	public static void main(String[] args) {
		tempIndex(true)
				.flatMap(index -> index
						.addDocument(new LLTerm("id", "123"),
								new LLDocument(new LLItem[]{
										LLItem.newStringField("id", "123", Store.YES),
										LLItem.newStringField("name", "Mario", Store.NO),
										LLItem.newStringField("surname", "Rossi", Store.NO)
								})
						)
						.then(index.refresh())
						.then(index.search(null, new TermQuery("name", "Mario"), 1, null, LLScoreMode.COMPLETE_NO_SCORES, "id"))
						.flatMap(results -> results
								.results()
								.flatMap(r -> r)
								.doOnNext(value -> System.out.println("Value: " + value))
								.then(results.totalHitsCount())
						)
						.doOnNext(count -> System.out.println("Total hits: " + count))
						.doOnTerminate(() -> System.out.println("Completed"))
						.then(index.close())
				)
				.subscribeOn(Schedulers.parallel())
				.block();
	}

	public static final class CurrentCustomType {

		private final int number;

		public CurrentCustomType(int number) {
			this.number = number;
		}

		public int getNumber() {
			return number;
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", CurrentCustomType.class.getSimpleName() + "[", "]")
					.add("number=" + number)
					.toString();
		}
	}

	private static <U> Mono<? extends LLLuceneIndex> tempIndex(boolean delete) {
		var wrkspcPath = Path.of("/tmp/tempdb/");
		return Mono
				.fromCallable(() -> {
					if (delete && Files.exists(wrkspcPath)) {
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
				.then(new LLLocalDatabaseConnection(wrkspcPath, true).connect())
				.flatMap(conn -> conn.getLuceneIndex("testindices",
						3,
						TextFieldsAnalyzer.PartialWords,
						Duration.ofSeconds(5),
						Duration.ofSeconds(5),
						false
				));
	}
}
