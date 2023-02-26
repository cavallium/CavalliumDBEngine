package it.cavallium.dbengine.tests;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.lucene.searcher.GlobalQueryRewrite;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneSearchResult;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

public class SwappableLuceneSearcher implements LocalSearcher, MultiSearcher, Closeable {

	private final AtomicReference<LocalSearcher> single = new AtomicReference<>(null);
	private final AtomicReference<MultiSearcher> multi = new AtomicReference<>(null);

	public SwappableLuceneSearcher() {

	}

	@Override
	public LuceneSearchResult collect(LLIndexSearcher indexSearcher,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		var single = this.single.get();
		if (single == null) {
			single = this.multi.get();
		}
		requireNonNull(single, "LuceneLocalSearcher not set");
		return single.collect(indexSearcher, queryParams, keyFieldName, transformer, filterer);
	}

	@Override
	public String getName() {
		var single = this.single.get();
		var multi = this.multi.get();
		if (single == multi) {
			if (single == null) {
				return "swappable";
			} else {
				return single.getName();
			}
		} else {
			return "swappable[single=" + single.getName() + ",multi=" + multi.getName() + "]";
		}
	}

	@Override
	public LuceneSearchResult collectMulti(LLIndexSearchers indexSearchers,
			LocalQueryParams queryParams,
			@Nullable String keyFieldName,
			GlobalQueryRewrite transformer,
			Function<Stream<LLKeyScore>, Stream<LLKeyScore>> filterer) {
		var multi = requireNonNull(this.multi.get(), "LuceneMultiSearcher not set");
		return multi.collectMulti(indexSearchers, queryParams, keyFieldName, transformer, filterer);
	}

	public void setSingle(LocalSearcher single) {
		this.single.set(single);
	}

	public void setMulti(MultiSearcher multi) {
		this.multi.set(multi);
	}

	@Override
	public void close() throws IOException {
		if (this.single.get() instanceof Closeable closeable) {
			closeable.close();
		}
		if (this.multi.get() instanceof Closeable closeable) {
			closeable.close();
		}
	}
}
