package it.cavallium.dbengine.lucene;

import java.io.IOException;
import java.math.BigInteger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.jetbrains.annotations.NotNull;
import org.warp.commonutils.random.LFSR.LFSRIterator;

public class RandomFieldComparator extends FieldComparator<Float> implements LeafFieldComparator {

	private final @NotNull LFSRIterator rand;
	private final float[] scores;
	private float bottom;
	private Scorable scorer;
	private float topValue;

	/** Creates a new comparator based on relevance for {@code numHits}. */
	public RandomFieldComparator(@NotNull LFSRIterator rand, int numHits) {
		this.rand = rand;
		scores = new float[numHits];
	}

	@Override
	public int compare(int slot1, int slot2) {
		return Float.compare(scores[slot2], scores[slot1]);
	}

	@Override
	public int compareBottom(int doc) throws IOException {
		float score = scorer.score();
		assert !Float.isNaN(score);
		return Float.compare(score, bottom);
	}

	@Override
	public void copy(int slot, int doc) throws IOException {
		scores[slot] = scorer.score();
		assert !Float.isNaN(scores[slot]);
	}

	@Override
	public LeafFieldComparator getLeafComparator(LeafReaderContext context) {
		return this;
	}

	@Override
	public void setBottom(final int bottom) {
		this.bottom = scores[bottom];
	}

	@Override
	public void setTopValue(Float value) {
		topValue = Float.MAX_VALUE;
	}

	@Override
	public void setScorer(Scorable scorer) {
		// wrap with a ScoreCachingWrappingScorer so that successive calls to
		// score() will not incur score computation over and
		// over again.
		var randomizedScorer = new Scorable() {

			@Override
			public float score() {
				return randomize(scorer.docID());
			}

			@Override
			public int docID() {
				return scorer.docID();
			}
		};
		this.scorer = ScoreCachingWrappingScorer.wrap(randomizedScorer);
	}

	@SuppressWarnings("RedundantCast")
	@Override
	public Float value(int slot) {
		return (float) scores[slot];
	}

	// Override because we sort reverse of natural Float order:
	@Override
	public int compareValues(Float first, Float second) {
		// Reversed intentionally because relevance by default
		// sorts descending:
		return second.compareTo(first);
	}

	@Override
	public int compareTop(int doc) throws IOException {
		float docValue = scorer.score();
		assert !Float.isNaN(docValue);
		return Float.compare(docValue, topValue);
	}

	private float randomize(int num) {
		int val = rand.next(BigInteger.valueOf(num)).intValue();
		return (val & 0x00FFFFFF) / (float)(1 << 24); // only use the lower 24 bits to construct a float from 0.0-1.0
	}
}
