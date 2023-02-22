package it.cavallium.dbengine.tests;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.dbengine.client.Indicizer;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.LLItem;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import java.util.LinkedList;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;

public class StringIndicizer extends Indicizer<String, String> {

	@Override
	public @NotNull LLUpdateDocument toIndexRequest(@NotNull String key, @NotNull String value) {
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
		return new LLUpdateDocument(fields);
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
	public @NotNull String getKey(IndexableField key) {
		return key.stringValue();
	}

	@Override
	public IndicizerAnalyzers getPerFieldAnalyzer() {
		return it.cavallium.dbengine.client.IndicizerAnalyzers.of(TextFieldsAnalyzer.ICUCollationKey);
	}

	@Override
	public IndicizerSimilarities getPerFieldSimilarity() {
		return it.cavallium.dbengine.client.IndicizerSimilarities.of(TextFieldsSimilarity.BM25Standard);
	}
}
