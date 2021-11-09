package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import it.cavallium.dbengine.lucene.FullDocs;
import it.cavallium.dbengine.lucene.LLDoc;
import it.cavallium.dbengine.lucene.LLFieldDoc;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TotalHits;
import reactor.core.publisher.Flux;

public class LMDBFullFieldDocCollectorMultiManager implements
		CollectorMultiManager<FullFieldDocs<LLFieldDoc>, FullFieldDocs<LLFieldDoc>> {


	private final Sort sort;
	private final CollectorManager<LMDBFullFieldDocCollector, FullFieldDocs<LLFieldDoc>> sharedCollector;

	public LMDBFullFieldDocCollectorMultiManager(LLTempLMDBEnv env, Sort sort, int limit, long totalHitsThreshold) {
		this.sort = sort;
		this.sharedCollector = LMDBFullFieldDocCollector.createSharedManager(env, sort, limit, totalHitsThreshold);
	}

	public CollectorManager<LMDBFullFieldDocCollector, FullFieldDocs<LLFieldDoc>> get(int shardIndex) {
		return new CollectorManager<>() {
			@Override
			public LMDBFullFieldDocCollector newCollector() throws IOException {
				return sharedCollector.newCollector();
			}

			@Override
			public FullFieldDocs<LLFieldDoc> reduce(Collection<LMDBFullFieldDocCollector> collectors) throws IOException {
				@SuppressWarnings("unchecked")
				final FullDocs<LLFieldDoc>[] fullDocs = new FullDocs[collectors.size()];
				int i = 0;
				for (var collector : collectors) {
					fullDocs[i++] = collector.fullDocs();
				}
				var result = (FullFieldDocs<LLFieldDoc>) FullDocs.merge(sort, fullDocs);
				return new FullFieldDocs<>(new FullDocs<>() {
					@Override
					public Flux<LLFieldDoc> iterate() {
						return result.iterate().map(doc -> new LLFieldDoc(doc.doc(), doc.score(), shardIndex, doc.fields()));
					}

					@Override
					public Flux<LLFieldDoc> iterate(long skips) {
						return result.iterate(skips).map(doc -> new LLFieldDoc(doc.doc(), doc.score(), shardIndex, doc.fields()));
					}

					@Override
					public TotalHits totalHits() {
						return result.totalHits();
					}
				}, result.fields());
			}
		};
	}

	@Override
	public ScoreMode scoreMode() {
		throw new NotImplementedException();
	}

	@Override
	public FullFieldDocs<LLFieldDoc> reduce(List<FullFieldDocs<LLFieldDoc>> results) {
		//noinspection unchecked
		return (FullFieldDocs<LLFieldDoc>) FullDocs
				.merge(sort, (FullDocs<LLFieldDoc>[]) results.toArray(FullDocs<?>[]::new));
	}
}
