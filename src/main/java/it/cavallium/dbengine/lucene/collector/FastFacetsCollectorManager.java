package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.lucene.IntSmear;
import it.unimi.dsi.fastutil.ints.IntHash;
import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

public class FastFacetsCollectorManager implements CollectorManager<FacetsCollector, FacetsCollector> {

	private final int collectionRate;
	private final IntHash.Strategy hash;
	private final FacetsCollectorManager facetsCollectorManager;

	public FastFacetsCollectorManager(int collectionRate) {
		this.collectionRate = collectionRate;
		this.hash = new IntSmear();
		this.facetsCollectorManager = new FacetsCollectorManager();
	}

	@Override
	public FacetsCollector newCollector() {
		return new FastFacetsCollector(collectionRate, hash);
	}

	@Override
	public FacetsCollector reduce(Collection<FacetsCollector> collectors) {
		return FacetsCollector.wrap(facetsCollectorManager.reduce(collectors
				.stream()
				.map(facetsCollector -> facetsCollector.getLuceneFacetsCollector())
				.toList()));
	}

	private static class FastFacetsCollector implements FacetsCollector {

		private final org.apache.lucene.facet.FacetsCollector collector;
		private final int collectionRate;
		private final IntHash.Strategy hash;

		public FastFacetsCollector(int collectionRate, IntHash.Strategy hash) {
			this.collectionRate = collectionRate;
			this.hash = hash;
			this.collector = new org.apache.lucene.facet.FacetsCollector(false) {
				@Override
				public ScoreMode scoreMode() {
					return ScoreMode.COMPLETE_NO_SCORES;
				}
			};
		}


		@Override
		public org.apache.lucene.facet.FacetsCollector getLuceneFacetsCollector() {
			return collector;
		}

		@Override
		public LeafCollector getLeafCollector(LeafReaderContext context) {
			var leafCollector = collector.getLeafCollector(context);
			return new LeafCollector() {
				@Override
				public void setScorer(Scorable scorer) {
					leafCollector.setScorer(scorer);
				}

				@Override
				public void collect(int doc) {
					if (collectionRate == 1 || hash.hashCode(doc) % collectionRate == 0) {
						leafCollector.collect(doc);
					}
				}

				@Override
				public DocIdSetIterator competitiveIterator() {
					return leafCollector.competitiveIterator();
				}
			};
		}

		@Override
		public ScoreMode scoreMode() {
			return collector.scoreMode();
		}
	}
}
