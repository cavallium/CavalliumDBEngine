package it.cavallium.dbengine.lucene;

import it.cavallium.dbengine.utils.LFSR;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;

public class RandomFieldComparatorSource extends FieldComparatorSource {

	private final LFSR rand;

	public RandomFieldComparatorSource() {
		this.rand = LFSR.random(24, ThreadLocalRandom.current().nextInt(1 << 24));
	}

	@Override
	public FieldComparator<?> newComparator(String fieldName, int numHits, boolean enableSkipping, boolean reversed) {
		return new RandomFieldComparator(rand.iterator(), numHits);
	}
}
