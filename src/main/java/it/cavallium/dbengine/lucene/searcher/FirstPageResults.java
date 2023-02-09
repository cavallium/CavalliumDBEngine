package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.LLKeyScore;
import java.util.stream.Stream;

record FirstPageResults(TotalHitsCount totalHitsCount, Stream<LLKeyScore> firstPageHitsStream,
												CurrentPageInfo nextPageInfo) {}
