package it.cavallium.dbengine.client;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import it.cavallium.dbengine.database.LLIndexRequest;
import it.cavallium.dbengine.database.LLSoftUpdateDocument;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUpdateDocument;
import it.cavallium.dbengine.database.LLUpdateFields;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import java.util.Map;
import org.apache.lucene.index.IndexableField;
import org.jetbrains.annotations.NotNull;

public abstract class Indicizer<T, U> {

	/**
	 * Transform a value to an IndexRequest.
	 */
	public abstract @NotNull LLIndexRequest toIndexRequest(@NotNull T key, @NotNull U value);

	public final @NotNull LLUpdateDocument toDocument(@NotNull T key, @NotNull U value) {
		var req = toIndexRequest(key, value);
		if (req instanceof LLUpdateFields updateFields) {
			return new LLUpdateDocument(updateFields.items());
		} else if (req instanceof LLUpdateDocument updateDocument) {
			return updateDocument;
		} else if (req instanceof LLSoftUpdateDocument softUpdateDocument) {
			return new LLUpdateDocument(softUpdateDocument.items());
		} else {
			throw new UnsupportedOperationException("Unexpected request type: " + req);
		}
	}

	public abstract @NotNull LLTerm toIndex(@NotNull T key);

	public abstract @NotNull String getKeyFieldName();

	public abstract @NotNull T getKey(IndexableField key);

	public abstract IndicizerAnalyzers getPerFieldAnalyzer();

	public abstract IndicizerSimilarities getPerFieldSimilarity();

	public Multimap<String, String> getMoreLikeThisDocumentFields(T key, U value) {
		return Multimaps.forMap(Map.of());
	}
}
