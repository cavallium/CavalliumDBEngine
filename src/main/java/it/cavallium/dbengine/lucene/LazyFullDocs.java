package it.cavallium.dbengine.lucene;

import it.cavallium.dbengine.utils.SimpleResource;
import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.search.TotalHits;
import reactor.core.publisher.Flux;

public class LazyFullDocs<T extends LLDoc> extends SimpleResource implements FullDocs<T> {

	private final ResourceIterable<T> pq;
	private final TotalHits totalHits;

	public LazyFullDocs(ResourceIterable<T> pq, TotalHits totalHits) {
		this.pq = pq;
		this.totalHits = totalHits;
	}

	@Override
	public Flux<T> iterate() {
		return pq.iterate();
	}

	@Override
	public Flux<T> iterate(long skips) {
		return pq.iterate(skips);
	}

	@Override
	public TotalHits totalHits() {
		return totalHits;
	}

	@Override
	protected void onClose() {
		pq.close();
	}
}
