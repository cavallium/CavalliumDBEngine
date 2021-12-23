package it.cavallium.dbengine.lucene.searcher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;

public class LuceneMultiGenerator implements Supplier<ScoreDoc> {

	private final Iterator<Supplier<ScoreDoc>> generators;
	private Supplier<ScoreDoc> luceneGenerator;

	public LuceneMultiGenerator(List<IndexSearcher> shards, LocalQueryParams localQueryParams) {
		this.generators = IntStream
				.range(0, shards.size())
				.mapToObj(shardIndex -> {
					IndexSearcher shard = shards.get(shardIndex);
					return (Supplier<ScoreDoc>) new LuceneGenerator(shard, localQueryParams, shardIndex);
				})
				.iterator();
		tryAdvanceGenerator();
	}

	private void tryAdvanceGenerator() {
		if (generators.hasNext()) {
			luceneGenerator = generators.next();
		} else {
			luceneGenerator = null;
		}
	}

	@Override
	public ScoreDoc get() {
		if (luceneGenerator == null) {
			return null;
		}
		ScoreDoc item;
		do {
			item = luceneGenerator.get();
			if (item == null) {
				tryAdvanceGenerator();
				if (luceneGenerator == null) {
					return null;
				}
			}
		} while (item == null);
		return item;
	}
}
