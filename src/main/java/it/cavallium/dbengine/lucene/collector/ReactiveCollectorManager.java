package it.cavallium.dbengine.lucene.collector;

import java.util.Collection;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Sinks.Many;

public class ReactiveCollectorManager implements CollectorManager<Collector, Void> {

	private final FluxSink<ScoreDoc> scoreDocsSink;

	public ReactiveCollectorManager(FluxSink<ScoreDoc> scoreDocsSink) {
		this.scoreDocsSink = scoreDocsSink;
	}

	@Override
	public ReactiveCollector newCollector() {
		return new ReactiveCollector(scoreDocsSink);
	}

	@Override
	public Void reduce(Collection<Collector> collection) {
		throw new UnsupportedOperationException();
	}
}
