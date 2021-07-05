package it.cavallium.dbengine.lucene;

import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.client.query.QueryParser;
import it.cavallium.dbengine.client.query.current.data.QueryParams;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionary;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import it.cavallium.dbengine.database.collections.Joiner.ValueGetter;
import it.cavallium.dbengine.lucene.analyzer.NCharGramAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.NCharGramEdgeAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.lucene.analyzer.TextFieldsSimilarity;
import it.cavallium.dbengine.lucene.analyzer.WordAnalyzer;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.similarity.NGramSimilarity;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.jetbrains.annotations.Nullable;
import org.novasearch.lucene.search.similarities.BM25Similarity;
import org.novasearch.lucene.search.similarities.BM25Similarity.BM25Model;
import org.novasearch.lucene.search.similarities.LdpSimilarity;
import org.novasearch.lucene.search.similarities.LtcSimilarity;
import org.novasearch.lucene.search.similarities.RobertsonSimilarity;
import org.warp.commonutils.log.Logger;

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
	private static final Analyzer luceneWordAnalyzerStopWordsAndStemInstance = new WordAnalyzer(false,true, true);
	private static final Analyzer luceneWordAnalyzerStopWordsInstance = new WordAnalyzer(false, true, false);
	private static final Analyzer luceneWordAnalyzerStemInstance = new WordAnalyzer(false, false, true);
	private static final Analyzer luceneWordAnalyzerSimpleInstance = new WordAnalyzer(false, false, false);
	private static final Analyzer luceneICUCollationKeyInstance = new WordAnalyzer(false, true, true);
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

	@SuppressWarnings("DuplicatedCode")
	public static Analyzer getAnalyzer(TextFieldsAnalyzer analyzer) {
		return switch (analyzer) {
			case N4GramPartialWords -> lucene4GramWordsAnalyzerInstance;
			case N4GramPartialString -> lucene4GramStringAnalyzerInstance;
			case N4GramPartialWordsEdge -> lucene4GramWordsAnalyzerEdgeInstance;
			case N4GramPartialStringEdge -> lucene4GramStringAnalyzerEdgeInstance;
			case N3To5GramPartialWords -> lucene3To5GramWordsAnalyzerInstance;
			case N3To5GramPartialString -> lucene3To5GramStringAnalyzerInstance;
			case N3To5GramPartialWordsEdge -> lucene3To5GramWordsAnalyzerEdgeInstance;
			case N3To5GramPartialStringEdge -> lucene3To5GramStringAnalyzerEdgeInstance;
			case Standard -> luceneStandardAnalyzerInstance;
			case FullText -> luceneWordAnalyzerStopWordsAndStemInstance;
			case WordWithStopwordsStripping -> luceneWordAnalyzerStopWordsInstance;
			case WordWithStemming -> luceneWordAnalyzerStemInstance;
			case WordSimple -> luceneWordAnalyzerSimpleInstance;
			case ICUCollationKey -> luceneICUCollationKeyInstance;
			//noinspection UnnecessaryDefault
			default -> throw new UnsupportedOperationException("Unknown analyzer: " + analyzer);
		};
	}

	@SuppressWarnings("DuplicatedCode")
	public static Similarity getSimilarity(TextFieldsSimilarity similarity) {
		return switch (similarity) {
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
	 *
	 * @return false if the result is not relevant
	 */
	@Nullable
	public static boolean filterTopDoc(float score, Float minCompetitiveScore) {
		return minCompetitiveScore == null || score >= minCompetitiveScore;
	}

	@Nullable
	public static String keyOfTopDoc(Logger logger, int docId, IndexReader indexReader,
			String keyFieldName) throws IOException {
		if (docId > indexReader.maxDoc()) {
			logger.warn("Document " + docId + " > maxDoc (" +indexReader.maxDoc() + ")");
			return null;
		}
		Document d = indexReader.document(docId, Set.of(keyFieldName));
		if (d.getFields().isEmpty()) {
			StringBuilder sb = new StringBuilder();
			sb.append("The document docId: ").append(docId).append(" is empty.");
			var realFields = indexReader.document(docId).getFields();
			if (!realFields.isEmpty()) {
				sb.append("\n");
				logger.error("Present fields:\n");
				boolean first = true;
				for (IndexableField field : realFields) {
					if (first) {
						first = false;
					} else {
						sb.append("\n");
					}
					sb.append(" - ").append(field.name());
				}
			}
			throw new IOException(sb.toString());
		} else {
			var field = d.getField(keyFieldName);
			if (field == null) {
				throw new IOException("Can't get key of document docId: " + docId);
			} else {
				return field.stringValue();
			}
		}
	}

	public static <T, U, V> ValueGetter<Entry<T, U>, V> getAsyncDbValueGetterDeep(
			CompositeSnapshot snapshot,
			DatabaseMapDictionaryDeep<T, Map<U, V>, DatabaseMapDictionary<U, V>> dictionaryDeep) {
		return entry -> dictionaryDeep
				.at(snapshot, entry.getKey())
				.flatMap(sub -> sub.getValue(snapshot, entry.getValue()).doAfterTerminate(sub::release));
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

	public static void readInternalAligned(Object ref, FileChannel channel, long pos, ByteBuffer b, int readLength, int usefulLength, long end) throws IOException {
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

			pos += (long)i;
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
	public static FieldDoc getLastFieldDoc(ScoreDoc[] scoreDocs) {
		if (scoreDocs == null) {
			return null;
		}
		if (scoreDocs.length == 0) {
			return null;
		}
		return (FieldDoc) scoreDocs[scoreDocs.length - 1];
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

	public static LocalQueryParams toLocalQueryParams(QueryParams queryParams) {
		return new LocalQueryParams(QueryParser.toQuery(queryParams.query()),
				safeLongToInt(queryParams.offset()),
				safeLongToInt(queryParams.limit()),
				queryParams.minCompetitiveScore().getNullable(),
				QueryParser.toSort(queryParams.sort()),
				QueryParser.toScoreMode(queryParams.scoreMode())
		);
	}
}
