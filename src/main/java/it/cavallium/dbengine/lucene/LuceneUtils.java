package it.cavallium.dbengine.lucene;

import static it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite.NO_REWRITE;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import it.cavallium.datagen.nativedata.Nullabledouble;
import it.cavallium.datagen.nativedata.Nullableint;
import it.cavallium.datagen.nativedata.Nullablelong;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import it.cavallium.dbengine.database.collections.DatabaseStageEntry;
import it.cavallium.dbengine.database.collections.DatabaseStageMap;
import it.cavallium.dbengine.database.collections.ValueGetter;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.LuceneConcurrentMergeScheduler.LuceneMergeThread;
import it.cavallium.dbengine.lucene.analyzer.LegacyWordAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.NCharGramAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.NCharGramEdgeAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.dbengine.lucene.analyzer.WordAnalyzer;
import it.cavallium.dbengine.lucene.mlt.BigCompositeReader;
import it.cavallium.dbengine.lucene.mlt.MultiMoreLikeThis;
import it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneSearchResult;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import it.cavallium.dbengine.lucene.similarity.NGramSimilarity;
import it.cavallium.dbengine.rpc.current.data.ByteBuffersDirectory;
import it.cavallium.dbengine.rpc.current.data.DirectIOFSDirectory;
import it.cavallium.dbengine.rpc.current.data.IndicizerAnalyzers;
import it.cavallium.dbengine.rpc.current.data.IndicizerSimilarities;
import it.cavallium.dbengine.rpc.current.data.LuceneDirectoryOptions;
import it.cavallium.dbengine.rpc.current.data.LuceneIndexStructure;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import it.cavallium.dbengine.rpc.current.data.MemoryMappedFSDirectory;
import it.cavallium.dbengine.rpc.current.data.NIOFSDirectory;
import it.cavallium.dbengine.rpc.current.data.NRTCachingDirectory;
import it.cavallium.dbengine.rpc.current.data.RAFFSDirectory;
import it.cavallium.dbengine.utils.DBException;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.misc.store.RAFDirectory;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.StringHelper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.novasearch.lucene.search.similarities.BM25Similarity;
import org.novasearch.lucene.search.similarities.BM25Similarity.BM25Model;
import org.novasearch.lucene.search.similarities.LdpSimilarity;
import org.novasearch.lucene.search.similarities.LtcSimilarity;
import org.novasearch.lucene.search.similarities.RobertsonSimilarity;

public class LuceneUtils {

	private static final Logger logger = LogManager.getLogger(LuceneUtils.class);

	private static final Analyzer luceneEdge4GramAnalyzerEdgeInstance = new NCharGramEdgeAnalyzer(4, 4);
	private static final Analyzer lucene4GramAnalyzerInstance = new NCharGramAnalyzer(4, 4);
	private static final Analyzer luceneEdge3To5GramAnalyzerEdgeInstance = new NCharGramEdgeAnalyzer(3, 5);
	private static final Analyzer lucene3To5GramAnalyzerInstance = new NCharGramAnalyzer(3, 5);
	private static final Analyzer luceneStandardAnalyzerInstance = new StandardAnalyzer();
	private static final Analyzer luceneWordAnalyzerLegacy1Instance = new LegacyWordAnalyzer(false, true, true);
	private static final Analyzer luceneWordAnalyzerLegacy2Instance = new LegacyWordAnalyzer(false, false, true);
	private static final Analyzer luceneWordAnalyzerLegacy3Instance = new LegacyWordAnalyzer(false, true, true);
	private static final Analyzer luceneWordAnalyzerStemInstance = new WordAnalyzer(false,true);
	private static final Analyzer luceneWordAnalyzerSimpleInstance = new WordAnalyzer(false, false);
	private static final Analyzer luceneICUCollationKeyInstance = new WordAnalyzer(true, true);
	private static final Similarity luceneBM25StandardSimilarityInstance = new org.apache.lucene.search.similarities.BM25Similarity();
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
	private static final Similarity luceneLTCSimilarityInstance = new LtcSimilarity();
	private static final Similarity luceneLDPSimilarityInstance = new LdpSimilarity();
	private static final Similarity luceneLDPNoLengthSimilarityInstance = new LdpSimilarity(0, 0.5f);
	private static final Similarity luceneBooleanSimilarityInstance = new BooleanSimilarity();
	private static final Similarity luceneRobertsonSimilarityInstance = new RobertsonSimilarity();
	// TODO: remove this default page limits and make the limits configurable into QueryParams
	private static final PageLimits DEFAULT_PAGE_LIMITS = new ExponentialPageLimits();
	private static final CharArraySet ENGLISH_AND_ITALIAN_STOP_WORDS;
	private static final LuceneIndexStructure SINGLE_STRUCTURE = new LuceneIndexStructure(1, IntList.of(0));
	private static final it.cavallium.dbengine.rpc.current.data.TieredMergePolicy DEFAULT_MERGE_POLICY = new it.cavallium.dbengine.rpc.current.data.TieredMergePolicy(
			Nullabledouble.empty(),
			Nullabledouble.empty(),
			Nullableint.empty(),
			Nullablelong.empty(),
			Nullablelong.empty(),
			Nullabledouble.empty(),
			Nullablelong.empty(),
			Nullabledouble.empty()
	);

	static {
		var cas = new CharArraySet(
				EnglishAnalyzer.ENGLISH_STOP_WORDS_SET.size() + ItalianAnalyzer.getDefaultStopSet().size(), true);
		cas.addAll(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
		cas.addAll(ItalianAnalyzer.getDefaultStopSet());
		ENGLISH_AND_ITALIAN_STOP_WORDS = CharArraySet.unmodifiableSet(cas);
	}

	@SuppressWarnings("DuplicatedCode")
	public static Analyzer getAnalyzer(TextFieldsAnalyzer analyzer) {
		return switch (analyzer) {
			case N4Gram -> lucene4GramAnalyzerInstance;
			case N4GramEdge -> luceneEdge4GramAnalyzerEdgeInstance;
			case N3To5Gram -> lucene3To5GramAnalyzerInstance;
			case N3To5GramEdge -> luceneEdge3To5GramAnalyzerEdgeInstance;
			case Standard -> luceneStandardAnalyzerInstance;
			case StandardMultilanguage -> luceneWordAnalyzerStemInstance;
			case LegacyFullText -> luceneWordAnalyzerLegacy1Instance;
			case LegacyWordWithStemming -> luceneWordAnalyzerLegacy2Instance;
			case LegacyICU -> luceneWordAnalyzerLegacy3Instance;
			case StandardSimple -> luceneWordAnalyzerSimpleInstance;
			case ICUCollationKey -> luceneICUCollationKeyInstance;
			//noinspection UnnecessaryDefault
			default -> throw new UnsupportedOperationException("Unknown analyzer: " + analyzer);
		};
	}

	@SuppressWarnings("DuplicatedCode")
	public static Similarity getSimilarity(TextFieldsSimilarity similarity) {
		return switch (similarity) {
			case BM25Standard -> luceneBM25StandardSimilarityInstance;
			case BM25Classic -> luceneBM25ClassicSimilarityInstance;
			case NGramBM25Classic -> luceneBM25ClassicNGramSimilarityInstance;
			case BM25L -> luceneBM25LSimilarityInstance;
			case NGramBM25L -> luceneBM25LNGramSimilarityInstance;
			case Classic -> luceneClassicSimilarityInstance;
			case NGramClassic -> luceneClassicNGramSimilarityInstance;
			case BM25Plus -> luceneBM25PlusSimilarityInstance;
			case NGramBM25Plus -> luceneBM25PlusNGramSimilarityInstance;
			case BM15Plus -> luceneBM15PlusSimilarityInstance;
			case NGramBM15Plus -> luceneBM15PlusNGramSimilarityInstance;
			case BM11Plus -> luceneBM11PlusSimilarityInstance;
			case NGramBM11Plus -> luceneBM11PlusNGramSimilarityInstance;
			case LTC -> luceneLTCSimilarityInstance;
			case LDP -> luceneLDPSimilarityInstance;
			case LDPNoLength -> luceneLDPNoLengthSimilarityInstance;
			case Robertson -> luceneRobertsonSimilarityInstance;
			case Boolean -> luceneBooleanSimilarityInstance;
			//noinspection UnnecessaryDefault
			default -> throw new IllegalStateException("Unknown similarity: " + similarity);
		};
	}

	/**
	 * @throws NoSuchElementException when the key is not found
	 * @throws IOException when an error occurs when reading the document
	 */
	@NotNull
	public static IndexableField keyOfTopDoc(int docId, IndexReader indexReader,
			String keyFieldName) throws NoSuchElementException, IOException {
		if (LLUtils.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called keyOfTopDoc in a nonblocking thread");
		}
		if (docId > indexReader.maxDoc()) {
			throw new DBException("Document " + docId + " > maxDoc (" +indexReader.maxDoc() + ")");
		}
		DocumentStoredSingleFieldVisitor visitor = new DocumentStoredSingleFieldVisitor(keyFieldName);
		indexReader.document(docId, visitor);
		Document d = visitor.getDocument();
		if (d.getFields().isEmpty()) {
			throw new NoSuchElementException(
					"Can't get key (field \"" + keyFieldName + "\") of document docId: " + docId + ". Available fields: []");
		} else {
			var field = d.getField(keyFieldName);
			if (field == null) {
				throw new NoSuchElementException(
						"Can't get key (field \"" + keyFieldName + "\") of document docId: " + docId + ". Available fields: " + d
								.getFields()
								.stream()
								.map(IndexableField::name)
								.collect(Collectors.joining(",", "[", "]")));
			} else {
				return field;
			}
		}
	}

	public static <T, U, V> ValueGetter<Entry<T, U>, V> getAsyncDbValueGetterDeep(
			CompositeSnapshot snapshot,
			DatabaseMapDictionaryDeep<T, Object2ObjectSortedMap<U, V>, ? extends DatabaseStageMap<U, V, ? extends DatabaseStageEntry<V>>> dictionaryDeep) {
		return entry -> dictionaryDeep.at(snapshot, entry.getKey()).getValue(snapshot, entry.getValue());
	}

	public static PerFieldAnalyzerWrapper toPerFieldAnalyzerWrapper(IndicizerAnalyzers indicizerAnalyzers) {
		HashMap<String, Analyzer> perFieldAnalyzer = new HashMap<>();
		indicizerAnalyzers
				.fieldAnalyzer()
				.forEach((key, value) -> perFieldAnalyzer.put(key, LuceneUtils.getAnalyzer(value)));
		return new PerFieldAnalyzerWrapper(LuceneUtils.getAnalyzer(indicizerAnalyzers.defaultAnalyzer()), perFieldAnalyzer);
	}

	public static PerFieldSimilarityWrapper toPerFieldSimilarityWrapper(IndicizerSimilarities indicizerSimilarities) {
		HashMap<String, Similarity> perFieldSimilarity = new HashMap<>();
		indicizerSimilarities
				.fieldSimilarity()
				.forEach((key, value) -> perFieldSimilarity.put(key, LuceneUtils.getSimilarity(value)));
		var defaultSimilarity = LuceneUtils.getSimilarity(indicizerSimilarities.defaultSimilarity());
		return new PerFieldSimilarityWrapper() {

			@Override
			public Similarity get(String name) {
				return perFieldSimilarity.getOrDefault(name, defaultSimilarity);
			}
		};
	}

	public static int alignUnsigned(int number, boolean expand) {
		if (number % 4096 != 0) {
			if (expand) {
				return number + (4096 - (number % 4096));
			} else {
				return number - (number % 4096);
			}
		} else {
			return number;
		}
	}

	public static long alignUnsigned(long number, boolean expand) {
		if (number % 4096L != 0) {
			if (expand) {
				return number + (4096L - (number % 4096L));
			} else {
				return number - (number % 4096L);
			}
		} else {
			return number;
		}
	}

	public static void readInternalAligned(Object ref,
			FileChannel channel,
			long pos,
			ByteBuffer b,
			int readLength,
			int usefulLength,
			long end) throws IOException {
		if (LLUtils.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called readInternalAligned in a nonblocking thread");
		}
		int startBufPosition = b.position();
		int readData = 0;
		int i;
		for(; readLength > 0; readLength -= i) {
			int toRead = readLength;
			b.limit(b.position() + toRead);

			assert b.remaining() == toRead;

			var beforeReadBufPosition = b.position();
			channel.read(b, pos);
			b.limit(Math.min(startBufPosition + usefulLength, b.position() + toRead));
			var afterReadBufPosition = b.position();
			i = (afterReadBufPosition - beforeReadBufPosition);
			readData += i;

			if (i < toRead && i > 0) {
				if (readData < usefulLength) {
					throw new EOFException("read past EOF: " + ref + " buffer: " + b + " chunkLen: " + toRead + " end: " + end);
				}
				if (readData == usefulLength) {
					b.limit(b.position());
					// File end reached
					return;
				}
			}

			if (i < 0) {
				throw new EOFException("read past EOF: " + ref + " buffer: " + b + " chunkLen: " + toRead + " end: " + end);
			}

			assert i > 0 : "FileChannel.read with non zero-length bb.remaining() must always read at least one byte (FileChannel is in blocking mode, see spec of ReadableByteChannel)";

			pos += i;
		}

		assert readLength == 0;
	}

	public static int safeLongToInt(long l) {
		if (l > 2147483630) {
			return 2147483630;
		} else if (l < -2147483630) {
			return -2147483630;
		} else {
			return (int) l;
		}
	}

	@Nullable
	public static ScoreDoc getLastScoreDoc(ScoreDoc[] scoreDocs) {
		if (scoreDocs == null) {
			return null;
		}
		if (scoreDocs.length == 0) {
			return null;
		}
		return scoreDocs[scoreDocs.length - 1];
	}

	public static LocalQueryParams toLocalQueryParams(QueryParams queryParams, Analyzer analyzer) {
		return new LocalQueryParams(QueryParser.toQuery(queryParams.query(), analyzer),
				queryParams.offset(),
				queryParams.limit(),
				DEFAULT_PAGE_LIMITS,
				QueryParser.toSort(queryParams.sort()),
				queryParams.computePreciseHitsCount(),
				Duration.ofMillis(queryParams.timeoutMilliseconds())
		);
	}

	public static Stream<LLKeyScore> convertHits(Stream<ScoreDoc> hitsFlux,
			List<IndexSearcher> indexSearchers,
			@Nullable String keyFieldName) {
		return hitsFlux.mapMulti((hit, sink) -> {
			var mapped = mapHitBlocking(hit, indexSearchers, keyFieldName);
			if (mapped != null) {
				sink.accept(mapped);
			}
		});
	}

	@Nullable
	private static LLKeyScore mapHitBlocking(ScoreDoc hit,
			List<IndexSearcher> indexSearchers,
			@Nullable String keyFieldName) {
		assert !LLUtils.isInNonBlockingThread();
		int shardDocId = hit.doc;
		int shardIndex = hit.shardIndex;
		float score = hit.score;
		IndexSearcher indexSearcher;
		if (shardIndex == -1 && indexSearchers.size() == 1) {
			indexSearcher = indexSearchers.get(0);
		} else {
			indexSearcher = indexSearchers.get(shardIndex);
		}
		try {
			IndexableField collectedDoc;
			if (keyFieldName != null) {
				collectedDoc = keyOfTopDoc(shardDocId, indexSearcher.getIndexReader(), keyFieldName);
			} else {
				collectedDoc = null;
			}
			return new LLKeyScore(shardDocId, shardIndex, score, collectedDoc);
		} catch (NoSuchElementException ex) {
			logger.debug("Error: document {} key is not present!", shardDocId);
			return null;
		} catch (Exception ex) {
			logger.error("Failed to read document {}", shardDocId, ex);
			return new LLKeyScore(shardDocId, shardIndex, score, null);
		}
	}

	public static TopDocs mergeTopDocs(
			@Nullable Sort sort,
			@Nullable Integer startN,
			@Nullable Integer topN,
			TopDocs[] topDocs) {
		if ((startN == null) != (topN == null)) {
			throw new IllegalArgumentException("You must pass startN and topN together or nothing");
		}
		TopDocs result;
		if (sort != null) {
			if (!(topDocs instanceof TopFieldDocs[])) {
				throw new IllegalStateException("Expected TopFieldDocs[], got TopDocs[]");
			}
			if (startN == null) {
				int defaultTopN = 0;
				for (TopDocs td : topDocs) {
					int length = td.scoreDocs.length;
					defaultTopN += length;
				}
				result = TopDocs.merge(sort, 0, defaultTopN,
						(TopFieldDocs[]) topDocs
				);
			} else {
				result = TopDocs.merge(sort, startN,
						topN,
						(TopFieldDocs[]) topDocs
				);
			}
		} else {
			if (startN == null) {
				int defaultTopN = 0;
				for (TopDocs td : topDocs) {
					int length = td.scoreDocs.length;
					defaultTopN += length;
				}
				result = TopDocs.merge(0,
						defaultTopN,
						topDocs
				);
			} else {
				result = TopDocs.merge(startN,
						topN,
						topDocs
				);
			}
		}
		return result;
	}

	public static int totalHitsThreshold(@Nullable Boolean complete) {
		return complete == null || complete ? Integer.MAX_VALUE : 1;
	}

	public static long totalHitsThresholdLong(@Nullable Boolean complete) {
		return complete == null || complete ? Long.MAX_VALUE : 1;
	}

	public static TotalHitsCount convertTotalHitsCount(TotalHits totalHits) {
		return switch (totalHits.relation) {
			case EQUAL_TO -> TotalHitsCount.of(totalHits.value, true);
			case GREATER_THAN_OR_EQUAL_TO -> TotalHitsCount.of(totalHits.value, false);
		};
	}

	public static TotalHitsCount sum(TotalHitsCount totalHitsCount, TotalHitsCount totalHitsCount1) {
		return TotalHitsCount.of(totalHitsCount.value() + totalHitsCount1.value(),
				totalHitsCount.exact() && totalHitsCount1.exact()
		);
	}

	@SuppressWarnings("unused")
	public static String toHumanReadableString(TotalHitsCount totalHitsCount) {
		if (totalHitsCount.exact()) {
			return Long.toString(totalHitsCount.value());
		} else {
			return totalHitsCount.value() + "+";
		}
	}

	public static Query getMoreLikeThisQuery(LLIndexSearchers inputIndexSearchers,
			LocalQueryParams localQueryParams,
			Analyzer analyzer,
			Similarity similarity,
			Multimap<String, String> mltDocumentFieldsMultimap) {
		List<IndexSearcher> indexSearchers = inputIndexSearchers.shards();
		Query luceneAdditionalQuery = localQueryParams.query();
		// Create the mutable version of the input
		Map<String, Collection<String>> mltDocumentFields = HashMultimap.create(mltDocumentFieldsMultimap).asMap();

		mltDocumentFields.entrySet().removeIf(entry -> entry.getValue().isEmpty());
		if (mltDocumentFields.isEmpty()) {
			return new MatchNoDocsQuery();
		}
		MultiMoreLikeThis mlt;
		if (indexSearchers.size() == 1) {
			mlt = new MultiMoreLikeThis(new BigCompositeReader<>(indexSearchers.get(0).getIndexReader(), IndexReader[]::new),
					null
			);
		} else {
			IndexReader[] indexReaders = new IndexReader[indexSearchers.size()];
			for (int i = 0, size = indexSearchers.size(); i < size; i++) {
				indexReaders[i] = indexSearchers.get(i).getIndexReader();
			}
			mlt = new MultiMoreLikeThis(new BigCompositeReader<>(indexReaders, new ArrayIndexComparator(indexReaders)), null);
		}
		mlt.setAnalyzer(analyzer);
		mlt.setFieldNames(mltDocumentFields.keySet().toArray(String[]::new));
		mlt.setMinTermFreq(1);
		mlt.setMinDocFreq(3);
		mlt.setMaxDocFreqPct(20);
		mlt.setBoost(localQueryParams.needsScores());
		mlt.setStopWords(ENGLISH_AND_ITALIAN_STOP_WORDS);
		if (similarity instanceof TFIDFSimilarity tfidfSimilarity) {
			mlt.setSimilarity(tfidfSimilarity);
		} else {
			mlt.setSimilarity(new ClassicSimilarity());
		}

		// Get the reference docId and apply it to MoreLikeThis, to generate the query
		Query mltQuery = null;
		try {
			mltQuery = mlt.like(mltDocumentFields);
		} catch (IOException e) {
			throw new DBException(e);
		}
		Query luceneQuery;
		if (!(luceneAdditionalQuery instanceof MatchAllDocsQuery)) {
			luceneQuery = new Builder()
					.add(mltQuery, Occur.MUST)
					.add(new ConstantScoreQuery(luceneAdditionalQuery), Occur.MUST)
					.build();
		} else {
			luceneQuery = mltQuery;
		}
		return luceneQuery;
	}

	public static Collector withTimeout(Collector collector, Duration timeout) {
		return new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), timeout.toMillis());
	}

	public static String getStandardName(String clusterName, int shardIndex) {
		return clusterName + "-shard" + shardIndex;
	}

	public static int getLuceneIndexId(LLTerm id, int totalShards) {
		return Math.abs(StringHelper.murmurhash3_x86_32(id.getValueBytesRef(), 7) % totalShards);
	}

	public static CheckOutputDirectory createLuceneDirectory(LuceneDirectoryOptions directoryOptions, String directoryName)
			throws IOException {
		return new CheckOutputDirectory(createLuceneDirectoryInternal(directoryOptions, directoryName));
	}

	private static Directory createLuceneDirectoryInternal(LuceneDirectoryOptions directoryOptions, String directoryName)
			throws IOException {
		Directory directory;
		if (directoryOptions instanceof ByteBuffersDirectory) {
			directory = new org.apache.lucene.store.ByteBuffersDirectory();
		} else if (directoryOptions instanceof DirectIOFSDirectory directIOFSDirectory) {
			FSDirectory delegateDirectory = (FSDirectory) createLuceneDirectoryInternal(directIOFSDirectory.delegate(),
					directoryName
			);
			if (Constants.LINUX || Constants.MAC_OS_X) {
				try {
					int mergeBufferSize = directIOFSDirectory.mergeBufferSize().orElse(DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE);
					long minBytesDirect = directIOFSDirectory.minBytesDirect().orElse(DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT);
					directory = new DirectIODirectory(delegateDirectory, mergeBufferSize, minBytesDirect);
				} catch (UnsupportedOperationException ex) {
					logger.warn("Failed to open FSDirectory with DIRECT flag", ex);
					directory = delegateDirectory;
				}
			} else {
				logger.warn("Failed to open FSDirectory with DIRECT flag because the operating system is Windows");
				directory = delegateDirectory;
			}
		} else if (directoryOptions instanceof MemoryMappedFSDirectory memoryMappedFSDirectory) {
			directory = new MMapDirectory(memoryMappedFSDirectory.managedPath().resolve(directoryName + ".lucene.db"));
		} else if (directoryOptions instanceof NIOFSDirectory niofsDirectory) {
			directory = new org.apache.lucene.store.NIOFSDirectory(niofsDirectory
					.managedPath()
					.resolve(directoryName + ".lucene.db"));
		} else if (directoryOptions instanceof RAFFSDirectory rafFsDirectory) {
			directory = new RAFDirectory(rafFsDirectory.managedPath().resolve(directoryName + ".lucene.db"));
		} else if (directoryOptions instanceof NRTCachingDirectory nrtCachingDirectory) {
			var delegateDirectory = createLuceneDirectoryInternal(nrtCachingDirectory.delegate(), directoryName);
			directory = new org.apache.lucene.store.NRTCachingDirectory(delegateDirectory,
					toMB(nrtCachingDirectory.maxMergeSizeBytes()),
					toMB(nrtCachingDirectory.maxCachedBytes())
			);
		} else {
			throw new UnsupportedOperationException("Unsupported directory: " + directoryName + ", " + directoryOptions);
		}
		return directory;
	}

	public static Optional<Path> getManagedPath(LuceneDirectoryOptions directoryOptions) {
		if (directoryOptions instanceof ByteBuffersDirectory) {
			return Optional.empty();
		} else if (directoryOptions instanceof DirectIOFSDirectory directIOFSDirectory) {
			return getManagedPath(directIOFSDirectory.delegate());
		} else if (directoryOptions instanceof MemoryMappedFSDirectory memoryMappedFSDirectory) {
			return Optional.of(memoryMappedFSDirectory.managedPath());
		} else if (directoryOptions instanceof NIOFSDirectory niofsDirectory) {
			return Optional.of(niofsDirectory.managedPath());
		} else if (directoryOptions instanceof RAFFSDirectory raffsDirectory) {
			return Optional.of(raffsDirectory.managedPath());
		} else if (directoryOptions instanceof NRTCachingDirectory nrtCachingDirectory) {
			return getManagedPath(nrtCachingDirectory.delegate());
		} else {
			throw new UnsupportedOperationException("Unsupported directory: " + directoryOptions);
		}
	}

	public static boolean getIsFilesystemCompressed(LuceneDirectoryOptions directoryOptions) {
		if (directoryOptions instanceof ByteBuffersDirectory) {
			return false;
		} else if (directoryOptions instanceof DirectIOFSDirectory directIOFSDirectory) {
			return getIsFilesystemCompressed(directIOFSDirectory.delegate());
		} else if (directoryOptions instanceof MemoryMappedFSDirectory) {
			return false;
		} else if (directoryOptions instanceof NIOFSDirectory) {
			return false;
		} else if (directoryOptions instanceof RAFFSDirectory) {
			return false;
		} else if (directoryOptions instanceof NRTCachingDirectory nrtCachingDirectory) {
			return getIsFilesystemCompressed(nrtCachingDirectory.delegate());
		} else {
			throw new UnsupportedOperationException("Unsupported directory: " + directoryOptions);
		}
	}

	public static IntList intListTo(int to) {
		var il = new IntArrayList(to);
		for (int i = 0; i < to; i++) {
			il.add(i);
		}
		return il;
	}

	public static LuceneIndexStructure singleStructure() {
		return SINGLE_STRUCTURE;
	}

	public static LuceneIndexStructure shardsStructure(int count) {
		return new LuceneIndexStructure(count, intListTo(count));
	}

	public static MergePolicy getMergePolicy(LuceneOptions luceneOptions) {
		var mergePolicy = new TieredMergePolicy();
		var mergePolicyOptions = luceneOptions.mergePolicy();
		if (mergePolicyOptions.deletesPctAllowed().isPresent()) {
			mergePolicy.setDeletesPctAllowed(mergePolicyOptions.deletesPctAllowed().get());
		}
		if (mergePolicyOptions.forceMergeDeletesPctAllowed().isPresent()) {
			mergePolicy.setForceMergeDeletesPctAllowed(mergePolicyOptions.forceMergeDeletesPctAllowed().get());
		}
		if (mergePolicyOptions.maxMergeAtOnce().isPresent()) {
			mergePolicy.setMaxMergeAtOnce(mergePolicyOptions.maxMergeAtOnce().get());
		}
		if (mergePolicyOptions.maxMergedSegmentBytes().isPresent()) {
			mergePolicy.setMaxMergedSegmentMB(toMB(mergePolicyOptions.maxMergedSegmentBytes().get()));
		}
		if (mergePolicyOptions.floorSegmentBytes().isPresent()) {
			mergePolicy.setFloorSegmentMB(toMB(mergePolicyOptions.floorSegmentBytes().get()));
		}
		if (mergePolicyOptions.segmentsPerTier().isPresent()) {
			mergePolicy.setSegmentsPerTier(mergePolicyOptions.segmentsPerTier().get());
		}
		if (mergePolicyOptions.maxCFSSegmentSizeBytes().isPresent()) {
			mergePolicy.setMaxCFSSegmentSizeMB(toMB(mergePolicyOptions.maxCFSSegmentSizeBytes().get()));
		}
		if (mergePolicyOptions.noCFSRatio().isPresent()) {
			mergePolicy.setNoCFSRatio(mergePolicyOptions.noCFSRatio().get());
		}
		return mergePolicy;
	}

	public static double toMB(long bytes) {
		if (bytes == Long.MAX_VALUE) return Double.MAX_VALUE;
		return ((double) bytes) / 1024D / 1024D;
	}

	public static it.cavallium.dbengine.rpc.current.data.TieredMergePolicy getDefaultMergePolicy() {
		return DEFAULT_MERGE_POLICY;
	}

	public static QueryParams getCountQueryParams(it.cavallium.dbengine.client.query.current.data.Query query) {
		return QueryParams.of(query, 0, 0, NoSort.of(), false, Long.MAX_VALUE);
	}

	/**
	 * Rewrite a lucene query of a local searcher, then call the local searcher again with the rewritten query
	 */
	public static LuceneSearchResult rewrite(LocalSearcher localSearcher,
			LLIndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		var indexSearchers = LLIndexSearchers.unsharded(indexSearcher);
		var queryParams2 = transformer.rewrite(indexSearchers, queryParams);
		return localSearcher.collect(indexSearcher, queryParams2, keyFieldName, NO_REWRITE, filterer);
	}

	/**
	 * Rewrite a lucene query of a multi searcher, then call the multi searcher again with the rewritten query
	 */
	public static LuceneSearchResult rewriteMulti(MultiSearcher multiSearcher,
			LLIndexSearchers indexSearchers,
			LocalQueryParams queryParams,
			String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		var queryParams2 = transformer.rewrite(indexSearchers, queryParams);
		return multiSearcher.collectMulti(indexSearchers, queryParams2, keyFieldName, NO_REWRITE, filterer);
	}

	public static void checkLuceneThread() {
		var thread = Thread.currentThread();
		if (!isLuceneThread()) {
			throw printLuceneThreadWarning(thread);
		}
	}

	@SuppressWarnings("ThrowableNotThrown")
	public static void warnLuceneThread() {
		var thread = Thread.currentThread();
		if (!isLuceneThread()) {
			printLuceneThreadWarning(thread);
		}
	}

	private static IllegalStateException printLuceneThreadWarning(Thread thread) {
		var error = new IllegalStateException("Current thread is not a lucene thread: " + thread.getId() + " " + thread
				+ ". Schedule it using LuceneUtils.luceneScheduler()");
		logger.warn("Current thread is not a lucene thread: {} {}", thread.getId(), thread, error);
		return error;
	}

	public static boolean isLuceneThread() {
		var thread = Thread.currentThread();
		return thread instanceof LuceneThread || thread instanceof LuceneMergeThread;
	}
}
