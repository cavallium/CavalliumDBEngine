package it.cavallium.dbengine;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.dbengine.client.Indicizer;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.LLItem;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import java.util.LinkedList;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

public class StringIndicizer extends Indicizer<String, String> {

	@Override
	public @NotNull Mono<LLUpdateDocument> toIndexRequest(@NotNull String key, @NotNull String value) {
		return Mono.fromCallable(() -> {
			var fields = new LinkedList<LLItem>();
			fields.add(LLItem.newStringField("uid", key, Field.Store.YES));
			fields.add(LLItem.newTextField("text", value, Store.NO));
			@SuppressWarnings("UnstableApiUsage")
			var numInt = Ints.tryParse(value);
			if (numInt != null) {
				fields.add(LLItem.newIntPoint("intpoint", numInt));
				fields.add(LLItem.newNumericDocValuesField("intsort", numInt));
			}
			@SuppressWarnings("UnstableApiUsage")
			var numLong = Longs.tryParse(value);
			if (numLong != null) {
				fields.add(LLItem.newLongPoint("longpoint", numLong));
				fields.add(LLItem.newNumericDocValuesField("longsort", numLong));
			}
			return new LLUpdateDocument(fields.toArray(LLItem[]::new));
		});
	}

	@Override
	public @NotNull LLTerm toIndex(@NotNull String key) {
		return new LLTerm("uid", key);
	}

	@Override
	public @NotNull String getKeyFieldName() {
		return "uid";
	}

	@Override
	public @NotNull String getKey(String key) {
		return key;
	}

	@Override
	public IndicizerAnalyzers getPerFieldAnalyzer() {
		return IndicizerAnalyzers.of(TextFieldsAnalyzer.ICUCollationKey);
	}

	@Override
	public IndicizerSimilarities getPerFieldSimilarity() {
		return IndicizerSimilarities.of(TextFieldsSimilarity.BM25Standard);
	}
}
