package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLDoc;
import it.cavallium.dbengine.lucene.LLFieldDoc;
import it.cavallium.dbengine.lucene.LLScoreDoc;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TotalHits;
import reactor.core.publisher.Flux;

public class LMDBFullScoreDocCollectorMultiManager implements
		CollectorMultiManager<FullDocs<LLScoreDoc>, FullDocs<LLScoreDoc>> {

	private final CollectorManager<LMDBFullScoreDocCollector, FullDocs<LLScoreDoc>> sharedCollector;

	public LMDBFullScoreDocCollectorMultiManager(LLTempLMDBEnv env, long limit, long totalHitsThreshold) {
		this.sharedCollector = LMDBFullScoreDocCollector.createSharedManager(env, limit, totalHitsThreshold);
	}

	public CollectorManager<LMDBFullScoreDocCollector, FullDocs<LLScoreDoc>> get(int shardIndex) {
		return new CollectorManager<>() {
			@Override
			public LMDBFullScoreDocCollector newCollector() throws IOException {
				return sharedCollector.newCollector();
			}

			@Override
			public FullDocs<LLScoreDoc> reduce(Collection<LMDBFullScoreDocCollector> collectors) throws IOException {
				var result = sharedCollector.reduce(collectors);
				return new FullDocs<>() {
					@Override
					public Flux<LLScoreDoc> iterate() {
						return result.iterate().map(doc -> new LLScoreDoc(doc.doc(), doc.score(), shardIndex));
					}

					@Override
					public Flux<LLScoreDoc> iterate(long skips) {
						return result.iterate(skips).map(doc -> new LLScoreDoc(doc.doc(), doc.score(), shardIndex));
					}

					@Override
					public TotalHits totalHits() {
						return result.totalHits();
					}
				};
			}
		};
	}

	@Override
	public ScoreMode scoreMode() {
		throw new NotImplementedException();
	}

	@Override
	public FullDocs<LLScoreDoc> reduce(List<FullDocs<LLScoreDoc>> results) {
		//noinspection unchecked
		return FullDocs.merge(Sort.RELEVANCE, (FullDocs<LLScoreDoc>[]) results.toArray(FullDocs<?>[]::new));
	}
}
