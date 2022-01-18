package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.lucene.IntSmear;
import it.unimi.dsi.fastutil.ints.IntHash;
import java.io.IOException;
import org.apache.lucene.facet.RandomSamplingFacetsCollector;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

public class FastRandomSamplingFacetsCollector extends SimpleCollector implements FacetsCollector {

	private final RandomSamplingFacetsCollector collector;
	private final int collectionRate;
	private final IntHash.Strategy hash;

	/**
	 * @param collectionRate collect 1 document every n collectable documents
	 */
	public FastRandomSamplingFacetsCollector(int collectionRate, int sampleSize) {
		this(collectionRate, sampleSize, 0);
	}

	public FastRandomSamplingFacetsCollector(int collectionRate, int sampleSize, long seed) {
		this.collectionRate = collectionRate;
		this.hash = new IntSmear();
		this.collector = new RandomSamplingFacetsCollector(sampleSize, seed) {
			@Override
			public ScoreMode scoreMode() {
				return ScoreMode.COMPLETE_NO_SCORES;
			}
		};
	}

	@Override
	protected void doSetNextReader(LeafReaderContext context) throws IOException {
		collector.getLeafCollector(context);
	}

	@Override
	public void setScorer(Scorable scorer) throws IOException {
		collector.setScorer(scorer);
	}

	@Override
	public void collect(int doc) throws IOException {
		if (hash.hashCode(doc) % collectionRate == 0) {
			collector.collect(doc);
		}
	}

	@Override
	public ScoreMode scoreMode() {
		return collector.scoreMode();
	}

	@Override
	public org.apache.lucene.facet.FacetsCollector getLuceneFacetsCollector() {
		return collector;
	}
}
