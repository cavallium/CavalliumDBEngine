package it.cavallium.dbengine.lucene;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.MultiSort;
import it.cavallium.dbengine.client.SearchResult;
import it.cavallium.dbengine.client.SearchResultItem;
import it.cavallium.dbengine.client.SearchResultKey;
import it.cavallium.dbengine.client.SearchResultKeys;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLSearchResultShard;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionary;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import it.cavallium.dbengine.lucene.analyzer.NCharGramAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.NCharGramEdgeAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.dbengine.lucene.analyzer.WordAnalyzer;
import it.cavallium.dbengine.lucene.searcher.LuceneStreamSearcher.HandleResult;
import it.cavallium.dbengine.lucene.searcher.LuceneStreamSearcher.ResultItemConsumer;
import it.cavallium.dbengine.lucene.similarity.NGramSimilarity;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.misc.SweetSpotSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.jetbrains.annotations.Nullable;
import org.novasearch.lucene.search.similarities.BM25Similarity;
import org.novasearch.lucene.search.similarities.BM25Similarity.BM25Model;
import org.novasearch.lucene.search.similarities.LdpSimilarity;
import org.novasearch.lucene.search.similarities.LtcSimilarity;
import org.novasearch.lucene.search.similarities.RobertsonSimilarity;
import org.warp.commonutils.log.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LuceneUtils {
	private static final Analyzer lucene4GramWordsAnalyzerEdgeInstance = new NCharGramEdgeAnalyzer(true, 4, 4);
	private static final Analyzer lucene4GramStringAnalyzerEdgeInstance = new NCharGramEdgeAnalyzer(false, 4, 4);
	private static final Analyzer lucene4GramWordsAnalyzerInstance = new NCharGramAnalyzer(true, 4, 4);
	private static final Analyzer lucene4GramStringAnalyzerInstance = new NCharGramAnalyzer(false, 4, 4);
	private static final Analyzer lucene3To5GramWordsAnalyzerEdgeInstance = new NCharGramEdgeAnalyzer(true, 3, 5);
	private static final Analyzer lucene3To5GramStringAnalyzerEdgeInstance = new NCharGramEdgeAnalyzer(false, 3, 5);
	private static final Analyzer lucene3To5GramWordsAnalyzerInstance = new NCharGramAnalyzer(true, 3, 5);
	private static final Analyzer lucene3To5GramStringAnalyzerInstance = new NCharGramAnalyzer(false, 3, 5);
	private static final Analyzer luceneStandardAnalyzerInstance = new StandardAnalyzer();
	private static final Analyzer luceneWordAnalyzerStopWordsAndStemInstance = new WordAnalyzer(true, true);
	private static final Analyzer luceneWordAnalyzerStopWordsInstance = new WordAnalyzer(true, false);
	private static final Analyzer luceneWordAnalyzerStemInstance = new WordAnalyzer(false, true);
	private static final Analyzer luceneWordAnalyzerSimpleInstance = new WordAnalyzer(false, false);
	private static final Similarity luceneBM25ClassicSimilarityInstance = new BM25Similarity(BM25Model.CLASSIC);
	private static final Similarity luceneBM25PlusSimilarityInstance = new BM25Similarity(BM25Model.PLUS);
	private static final Similarity luceneBM25LSimilarityInstance = new BM25Similarity(BM25Model.L);
	private static final Similarity luceneBM15PlusSimilarityInstance = new BM25Similarity(1.2f, 0.0f, 0.5f, BM25Model.PLUS);
	private static final Similarity luceneBM11PlusSimilarityInstance = new BM25Similarity(1.2f, 1.0f, 0.5f, BM25Model.PLUS);
	private static final Similarity luceneBM25ClassicNGramSimilarityInstance = NGramSimilarity.bm25(BM25Model.CLASSIC);
	private static final Similarity luceneBM25PlusNGramSimilarityInstance = NGramSimilarity.bm25(BM25Model.PLUS);
	private static final Similarity luceneBM25LNGramSimilarityInstance = NGramSimilarity.bm25(BM25Model.L);
	private static final Similarity luceneBM15PlusNGramSimilarityInstance = NGramSimilarity.bm15(BM25Model.PLUS);
	private static final Similarity luceneBM11PlusNGramSimilarityInstance = NGramSimilarity.bm11(BM25Model.PLUS);
	private static final Similarity luceneClassicSimilarityInstance = new ClassicSimilarity();
	private static final Similarity luceneClassicNGramSimilarityInstance = NGramSimilarity.classic();
	private static final Similarity luceneSweetSpotSimilarityInstance = new SweetSpotSimilarity();
	private static final Similarity luceneLTCSimilarityInstance = new LtcSimilarity();
	private static final Similarity luceneLDPSimilarityInstance = new LdpSimilarity();
	private static final Similarity luceneLDPNoLengthSimilarityInstance = new LdpSimilarity(0, 0.5f);
	private static final Similarity luceneBooleanSimilarityInstance = new BooleanSimilarity();
	private static final Similarity luceneRobertsonSimilarityInstance = new RobertsonSimilarity();

	public static Analyzer getAnalyzer(TextFieldsAnalyzer analyzer) {
		switch (analyzer) {
			case N4GramPartialWords:
				return lucene4GramWordsAnalyzerInstance;
			case N4GramPartialString:
				return lucene4GramStringAnalyzerInstance;
			case N4GramPartialWordsEdge:
				return lucene4GramWordsAnalyzerEdgeInstance;
			case N4GramPartialStringEdge:
				return lucene4GramStringAnalyzerEdgeInstance;
			case N3To5GramPartialWords:
				return lucene3To5GramWordsAnalyzerInstance;
			case N3To5GramPartialString:
				return lucene3To5GramStringAnalyzerInstance;
			case N3To5GramPartialWordsEdge:
				return lucene3To5GramWordsAnalyzerEdgeInstance;
			case N3To5GramPartialStringEdge:
				return lucene3To5GramStringAnalyzerEdgeInstance;
			case Standard:
				return luceneStandardAnalyzerInstance;
			case FullText:
				return luceneWordAnalyzerStopWordsAndStemInstance;
			case WordWithStopwordsStripping:
				return luceneWordAnalyzerStopWordsInstance;
			case WordWithStemming:
				return luceneWordAnalyzerStemInstance;
			case WordSimple:
				return luceneWordAnalyzerSimpleInstance;
			default:
				throw new UnsupportedOperationException("Unknown analyzer: " + analyzer);
		}
	}

	public static Similarity getSimilarity(TextFieldsSimilarity similarity) {
		switch (similarity) {
			case BM25Classic:
				return luceneBM25ClassicSimilarityInstance;
			case NGramBM25Classic:
				return luceneBM25ClassicNGramSimilarityInstance;
			case BM25L:
				return luceneBM25LSimilarityInstance;
			case NGramBM25L:
				return luceneBM25LNGramSimilarityInstance;
			case Classic:
				return luceneClassicSimilarityInstance;
			case NGramClassic:
				return luceneClassicNGramSimilarityInstance;
			case BM25Plus:
				return luceneBM25PlusSimilarityInstance;
			case NGramBM25Plus:
				return luceneBM25PlusNGramSimilarityInstance;
			case BM15Plus:
				return luceneBM15PlusSimilarityInstance;
			case NGramBM15Plus:
				return luceneBM15PlusNGramSimilarityInstance;
			case BM11Plus:
				return luceneBM11PlusSimilarityInstance;
			case NGramBM11Plus:
				return luceneBM11PlusNGramSimilarityInstance;
			case SweetSpot:
				return luceneSweetSpotSimilarityInstance;
			case LTC:
				return luceneLTCSimilarityInstance;
			case LDP:
				return luceneLDPSimilarityInstance;
			case LDPNoLength:
				return luceneLDPNoLengthSimilarityInstance;
			case Robertson:
				return luceneRobertsonSimilarityInstance;
			case Boolean:
				return luceneBooleanSimilarityInstance;
			default:
				throw new IllegalStateException("Unknown similarity: " + similarity);
		}
	}

	/**
	 *
	 * @param stem Enable stem filters on words.
	 *              Pass false if it will be used with a n-gram filter
	 */
	public static TokenStream newCommonFilter(TokenStream tokenStream, boolean stem) {
		tokenStream = newCommonNormalizer(tokenStream);
		if (stem) {
			tokenStream = new KStemFilter(tokenStream);
			tokenStream = new EnglishPossessiveFilter(tokenStream);
		}
		return tokenStream;
	}

	public static TokenStream newCommonNormalizer(TokenStream tokenStream) {
		tokenStream = new ASCIIFoldingFilter(tokenStream);
		tokenStream = new LowerCaseFilter(tokenStream);
		return tokenStream;
	}

	/**
	 * Merge streams together maintaining absolute order
	 */
	public static <T> Flux<T> mergeStream(Flux<Flux<T>> mappedMultiResults,
			@Nullable MultiSort<T> sort,
			long offset,
			@Nullable Long limit) {
		if (limit != null && limit == 0) {
			return mappedMultiResults.flatMap(f -> f).ignoreElements().flux();
		} else {
			return mappedMultiResults.collectList().flatMapMany(mappedMultiResultsList -> {
				Flux<T> mergedFlux;
				if (sort == null) {
					mergedFlux = Flux.merge(mappedMultiResultsList);
				} else {
					//noinspection unchecked
					mergedFlux = Flux.mergeOrdered(32, sort.getResultSort(), mappedMultiResultsList.toArray(Flux[]::new));
				}
				Flux<T> offsetedFlux;
				if (offset > 0) {
					offsetedFlux = mergedFlux.skip(offset);
				} else {
					offsetedFlux = mergedFlux;
				}
				if (limit == null || limit == Long.MAX_VALUE) {
					return offsetedFlux;
				} else {
					return offsetedFlux.limitRequest(limit);
				}
			});
		}
	}

	public static HandleResult collectTopDoc(Logger logger,
			int docId,
			float score,
			Float minCompetitiveScore,
			IndexSearcher indexSearcher,
			String keyFieldName,
			ResultItemConsumer resultsConsumer) throws IOException {
		if (minCompetitiveScore == null || score >= minCompetitiveScore) {
			Document d = indexSearcher.doc(docId, Set.of(keyFieldName));
			if (d.getFields().isEmpty()) {
				logger.error("The document docId: {}, score: {} is empty.", docId, score);
				var realFields = indexSearcher.doc(docId).getFields();
				if (!realFields.isEmpty()) {
					logger.error("Present fields:");
					for (IndexableField field : realFields) {
						logger.error(" - {}", field.name());
					}
				}
			} else {
				var field = d.getField(keyFieldName);
				if (field == null) {
					logger.error("Can't get key of document docId: {}, score: {}", docId, score);
				} else {
					if (resultsConsumer.accept(new LLKeyScore(field.stringValue(), score)) == HandleResult.HALT) {
						return HandleResult.HALT;
					}
				}
			}
		}
		return HandleResult.CONTINUE;
	}

	public static <T> Mono<SearchResultKeys<T>> mergeSignalStreamKeys(Flux<SearchResultKeys<T>> mappedKeys,
			MultiSort<SearchResultKey<T>> sort,
			long offset,
			Long limit) {
		return mappedKeys.reduce(
				new SearchResultKeys<>(Flux.empty(), 0L),
				(a, b) -> new SearchResultKeys<>(LuceneUtils.mergeStream(Flux.just(a.getResults(), b.getResults()),
						sort,
						offset,
						limit
				), a.getTotalHitsCount() + b.getTotalHitsCount())
		);
	}

	public static <T, U> Mono<SearchResult<T, U>> mergeSignalStreamItems(Flux<SearchResult<T, U>> mappedKeys,
			MultiSort<SearchResultItem<T, U>> sort,
			long offset,
			Long limit) {
		return mappedKeys.reduce(
				new SearchResult<>(Flux.empty(), 0L),
				(a, b) -> new SearchResult<>(LuceneUtils.mergeStream(Flux.just(a.getResults(), b.getResults()),
						sort,
						offset,
						limit
				), a.getTotalHitsCount() + b.getTotalHitsCount())
		);
	}

	public static Mono<LLSearchResultShard> mergeSignalStreamRaw(Flux<LLSearchResultShard> mappedKeys,
			MultiSort<LLKeyScore> mappedSort,
			long offset,
			Long limit) {
		return mappedKeys.reduce(
				new LLSearchResultShard(Flux.empty(), 0),
				(s1, s2) -> new LLSearchResultShard(
						LuceneUtils.mergeStream(Flux.just(s1.getResults(), s2.getResults()), mappedSort, offset, limit),
						s1.getTotalHitsCount() + s2.getTotalHitsCount()
				)
		);
	}

	public static <T, U, V> ValueGetter<Entry<T, U>, V> getAsyncDbValueGetterDeep(
			CompositeSnapshot snapshot,
			DatabaseMapDictionaryDeep<T, Map<U, V>, DatabaseMapDictionary<U, V>> dictionaryDeep) {
		return entry -> dictionaryDeep
				.at(snapshot, entry.getKey())
				.flatMap(sub -> sub.getValue(snapshot, entry.getValue()));
	}
}
