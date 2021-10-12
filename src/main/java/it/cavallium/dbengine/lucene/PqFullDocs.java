package it.cavallium.dbengine.lucene;

import org.apache.lucene.search.TotalHits;
import reactor.core.publisher.Flux;

public class PqFullDocs<T extends LLDocElement> implements FullDocs<T> {

	private final PriorityQueue<T> pq;
	private final TotalHits totalHits;

	public PqFullDocs(PriorityQueue<T> pq, TotalHits totalHits) {
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
}
