package it.cavallium.dbengine.lucene.collector;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;

public interface FacetsCollector extends Collector {

	static FacetsCollector wrap(org.apache.lucene.facet.FacetsCollector facetsCollector) {
		return new FacetsCollector() {

			@Override
			public org.apache.lucene.facet.FacetsCollector getLuceneFacetsCollector() {
				return facetsCollector;
			}

			@Override
			public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
				return facetsCollector.getLeafCollector(context);
			}

			@Override
			public ScoreMode scoreMode() {
				return facetsCollector.scoreMode();
			}
		};
	}

	org.apache.lucene.facet.FacetsCollector getLuceneFacetsCollector();
}
