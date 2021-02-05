package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLItem;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLScoreMode;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.lucene.serializer.Query;
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
										LLItem.newTextField("name", "Mario", Store.NO),
										LLItem.newStringField("surname", "Rossi", Store.NO)
								})
						)
						.then(index.refresh())
						.then(index.search(null, Query.exactSearch(TextFieldsAnalyzer.NGramPartialString,"name", "Mario"), 1, null, LLScoreMode.COMPLETE, "id"))
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
		tempIndex(true)
				.flatMap(index ->
						index
								.addDocument(new LLTerm("id", "126"),
										new LLDocument(new LLItem[]{
												LLItem.newStringField("id", "126", Store.YES),
												LLItem.newTextField("name", "Marioxq", Store.NO),
												LLItem.newStringField("surname", "Rossi", Store.NO)
										})
								)
								.then(index
						.addDocument(new LLTerm("id", "123"),
								new LLDocument(new LLItem[]{
										LLItem.newStringField("id", "123", Store.YES),
										LLItem.newTextField("name", "Mario", Store.NO),
										LLItem.newStringField("surname", "Rossi", Store.NO)
								})
						))
						.then(index
								.addDocument(new LLTerm("id", "124"),
										new LLDocument(new LLItem[]{
												LLItem.newStringField("id", "124", Store.YES),
												LLItem.newTextField("name", "Mariossi", Store.NO),
												LLItem.newStringField("surname", "Rossi", Store.NO)
										})
								))
						.then(index
								.addDocument(new LLTerm("id", "125"),
										new LLDocument(new LLItem[]{
												LLItem.newStringField("id", "125", Store.YES),
												LLItem.newTextField("name", "Mario marios", Store.NO),
												LLItem.newStringField("surname", "Rossi", Store.NO)
										})
								))
						.then(index
								.addDocument(new LLTerm("id", "128"),
										new LLDocument(new LLItem[]{
												LLItem.newStringField("id", "128", Store.YES),
												LLItem.newTextField("name", "Marion", Store.NO),
												LLItem.newStringField("surname", "Rossi", Store.NO)
										})
								))
						.then(index
								.addDocument(new LLTerm("id", "127"),
										new LLDocument(new LLItem[]{
												LLItem.newStringField("id", "127", Store.YES),
												LLItem.newTextField("name", "Mariotto", Store.NO),
												LLItem.newStringField("surname", "Rossi", Store.NO)
										})
								))
						.then(index.refresh())
						.then(index.search(null, Query.exactSearch(TextFieldsAnalyzer.NGramPartialString,"name", "Mario"), 10, MultiSort.topScore()
								.getQuerySort(), LLScoreMode.COMPLETE, "id"))
						.flatMap(results -> LuceneUtils.mergeStream(results
								.results(), MultiSort.topScoreRaw(), 10)
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
						10,
						TextFieldsAnalyzer.NGramPartialString,
						TextFieldsSimilarity.NGramBM25Plus,
						Duration.ofSeconds(5),
						Duration.ofSeconds(5),
						false
				));
	}
}
