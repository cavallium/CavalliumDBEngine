package it.cavallium.dbengine;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;

import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.disk.LLIndexSearcher;
import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import it.cavallium.dbengine.database.disk.LLLocalSingleton;
import it.cavallium.dbengine.lucene.searcher.LLSearchTransformer;
import it.cavallium.dbengine.lucene.searcher.LocalQueryParams;
import it.cavallium.dbengine.lucene.searcher.LuceneLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneMultiSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneSearchResult;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;

public class SwappableLuceneSearcher implements LuceneLocalSearcher, LuceneMultiSearcher, Closeable {

	private final AtomicReference<LuceneLocalSearcher> single = new AtomicReference<>(null);
	private final AtomicReference<LuceneMultiSearcher> multi = new AtomicReference<>(null);

	public SwappableLuceneSearcher() {

	}

	@Override
	public Mono<Send<LuceneSearchResult>> collect(Mono<Send<LLIndexSearcher>> indexSearcherMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		var single = requireNonNullElseGet(this.single.get(), this.multi::get);
		requireNonNull(single, "LuceneLocalSearcher not set");
		return single.collect(indexSearcherMono, queryParams, keyFieldName, transformer);
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
	public Mono<Send<LuceneSearchResult>> collectMulti(Mono<Send<LLIndexSearchers>> indexSearchersMono,
			LocalQueryParams queryParams,
			String keyFieldName,
			LLSearchTransformer transformer) {
		var multi = requireNonNull(this.multi.get(), "LuceneMultiSearcher not set");
		return multi.collectMulti(indexSearchersMono, queryParams, keyFieldName, transformer);
	}

	public void setSingle(LuceneLocalSearcher single) {
		this.single.set(single);
	}

	public void setMulti(LuceneMultiSearcher multi) {
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
