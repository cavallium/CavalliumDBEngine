package it.cavallium.dbengine.lucene.comparators;

import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempHugePqEnv;
import it.cavallium.dbengine.lucene.ByteArrayCodec;
import it.cavallium.dbengine.lucene.IArray;
import it.cavallium.dbengine.lucene.IntCodec;
import it.cavallium.dbengine.lucene.HugePqArray;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;

/**
 * Sorts by field's natural Term sort order, using ordinals. This is functionally equivalent to
 * {@link org.apache.lucene.search.FieldComparator.TermValComparator}, but it first resolves the
 * string to their relative ordinal positions (using the index returned by {@link
 * org.apache.lucene.index.LeafReader#getSortedDocValues(String)}), and does most comparisons
 * using the ordinals. For medium to large results, this comparator will be much faster than
 * {@link org.apache.lucene.search.FieldComparator.TermValComparator}. For very small result sets
 * it may be slower.
 * Based on {@link org.apache.lucene.search.FieldComparator.TermOrdValComparator}
 */
public class TermOrdValComparator extends FieldComparator<BytesRef> implements LeafFieldComparator, SafeCloseable {
	/* Ords for each slot.
	@lucene.internal */
	final IArray<Integer> ords;

	/* Values for each slot.
	@lucene.internal */
	final IArray<byte[]> values;

	/* Which reader last copied a value into the slot. When
	we compare two slots, we just compare-by-ord if the
	readerGen is the same; else we must compare the
	values (slower).
	@lucene.internal */
	final IArray<Integer> readerGen;

	/* Gen of current reader we are on.
	@lucene.internal */
	int currentReaderGen = -1;

	/* Current reader's doc ord/values.
	@lucene.internal */
	SortedDocValues termsIndex;

	private final String field;

	/* Bottom slot, or -1 if queue isn't full yet
	@lucene.internal */
	int bottomSlot = -1;

	/* Bottom ord (same as ords[bottomSlot] once bottomSlot
	is set).  Cached for faster compares.
	@lucene.internal */
	int bottomOrd;

	/* True if current bottom slot matches the current
	reader.
	@lucene.internal */
	boolean bottomSameReader;

	/* Bottom value (same as values[bottomSlot] once
	 bottomSlot is set).  Cached for faster compares.
	@lucene.internal */
	byte[] bottomValue;

	/** Set by setTopValue. */
	byte[] topValue;

	boolean topSameReader;
	int topOrd;

	/** -1 if missing values are sorted first, 1 if they are sorted last */
	final int missingSortCmp;

	/** Which ordinal to use for a missing value. */
	final int missingOrd;

	/** Creates this, sorting missing values first. */
	public TermOrdValComparator(LLTempHugePqEnv env, int numHits, String field) {
		this(env, numHits, field, false);
	}

	/**
	 * Creates this, with control over how missing values are sorted. Pass sortMissingLast=true to
	 * put missing values at the end.
	 */
	public TermOrdValComparator(LLTempHugePqEnv env, int numHits, String field, boolean sortMissingLast) {
		ords = new HugePqArray<>(env, new IntCodec(), numHits, 0);
		values = new HugePqArray<>(env, new ByteArrayCodec(), numHits, null);
		readerGen = new HugePqArray<>(env, new IntCodec(), numHits, 0);
		this.field = field;
		if (sortMissingLast) {
			missingSortCmp = 1;
			missingOrd = Integer.MAX_VALUE;
		} else {
			missingSortCmp = -1;
			missingOrd = -1;
		}
	}

	private int getOrdForDoc(int doc) throws IOException {
		if (termsIndex.advanceExact(doc)) {
			return termsIndex.ordValue();
		} else {
			return -1;
		}
	}

	@Override
	public int compare(int slot1, int slot2) {
		if ((int) readerGen.getOrDefault(slot2, 0) == readerGen.getOrDefault(slot1, 0)) {
			return ords.getOrDefault(slot1, 0) - ords.getOrDefault(slot2, 0);
		}

		final var val1 = values.get(slot1);
		final var val2 = values.get(slot2);
		if (val1 == null) {
			if (val2 == null) {
				return 0;
			}
			return missingSortCmp;
		} else if (val2 == null) {
			return -missingSortCmp;
		}
		return Arrays.compare(val1, val2);
	}

	@Override
	public int compareBottom(int doc) throws IOException {
		assert bottomSlot != -1;
		int docOrd = getOrdForDoc(doc);
		if (docOrd == -1) {
			docOrd = missingOrd;
		}
		if (bottomSameReader) {
			// ord is precisely comparable, even in the equal case
			return bottomOrd - docOrd;
		} else if (bottomOrd >= docOrd) {
			// the equals case always means bottom is > doc
			// (because we set bottomOrd to the lower bound in
			// setBottom):
			return 1;
		} else {
			return -1;
		}
	}

	@Override
	public void copy(int slot, int doc) throws IOException {
		int ord = getOrdForDoc(doc);
		if (ord == -1) {
			ord = missingOrd;
			values.reset(slot);
		} else {
			assert ord >= 0;
			values.set(slot, copyBytes(termsIndex.lookupOrd(ord)));
		}
		ords.set(slot, ord);
		readerGen.set(slot, currentReaderGen);
	}

	private byte[] copyBytes(BytesRef lookupOrd) {
		if (lookupOrd == null) return null;
		return Arrays.copyOfRange(lookupOrd.bytes, lookupOrd.offset, lookupOrd.length);
	}

	/** Retrieves the SortedDocValues for the field in this segment */
	protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field)
			throws IOException {
		return DocValues.getSorted(context.reader(), field);
	}

	@Override
	public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
		termsIndex = getSortedDocValues(context, field);
		currentReaderGen++;

		if (topValue != null) {
			// Recompute topOrd/SameReader
			int ord = termsIndex.lookupTerm(new BytesRef(topValue));
			if (ord >= 0) {
				topSameReader = true;
				topOrd = ord;
			} else {
				topSameReader = false;
				topOrd = -ord - 2;
			}
		} else {
			topOrd = missingOrd;
			topSameReader = true;
		}
		// System.out.println("  getLeafComparator topOrd=" + topOrd + " topSameReader=" +
		// topSameReader);

		if (bottomSlot != -1) {
			// Recompute bottomOrd/SameReader
			setBottom(bottomSlot);
		}

		return this;
	}

	@Override
	public void setBottom(final int bottom) throws IOException {
		bottomSlot = bottom;

		bottomValue = values.get(bottomSlot);
		if (currentReaderGen == readerGen.getOrDefault(bottomSlot, 0)) {
			bottomOrd = ords.getOrDefault(bottomSlot, 0);
			bottomSameReader = true;
		} else {
			if (bottomValue == null) {
				// missingOrd is null for all segments
				assert ords.getOrDefault(bottomSlot, 0) == missingOrd;
				bottomOrd = missingOrd;
				bottomSameReader = true;
				readerGen.set(bottomSlot, currentReaderGen);
			} else {
				final int ord = termsIndex.lookupTerm(new BytesRef(bottomValue));
				if (ord < 0) {
					bottomOrd = -ord - 2;
					bottomSameReader = false;
				} else {
					bottomOrd = ord;
					// exact value match
					bottomSameReader = true;
					readerGen.set(bottomSlot, currentReaderGen);
					ords.set(bottomSlot, bottomOrd);
				}
			}
		}
	}

	@Override
	public void setTopValue(BytesRef value) {
		// null is fine: it means the last doc of the prior
		// search was missing this value
		topValue = copyBytes(value);
		// System.out.println("setTopValue " + topValue);
	}

	@Override
	public BytesRef value(int slot) {
		return getBytesRef(values.get(slot));
	}

	private BytesRef getBytesRef(byte[] bytes) {
		if (bytes == null) return null;
		return new BytesRef(bytes);
	}

	@Override
	public int compareTop(int doc) throws IOException {

		int ord = getOrdForDoc(doc);
		if (ord == -1) {
			ord = missingOrd;
		}

		if (topSameReader) {
			// ord is precisely comparable, even in the equal
			// case
			// System.out.println("compareTop doc=" + doc + " ord=" + ord + " ret=" + (topOrd-ord));
			return topOrd - ord;
		} else if (ord <= topOrd) {
			// the equals case always means doc is < value
			// (because we set lastOrd to the lower bound)
			return 1;
		} else {
			return -1;
		}
	}

	@Override
	public int compareValues(BytesRef val1, BytesRef val2) {
		if (val1 == null) {
			if (val2 == null) {
				return 0;
			}
			return missingSortCmp;
		} else if (val2 == null) {
			return -missingSortCmp;
		}
		return val1.compareTo(val2);
	}

	@Override
	public void setScorer(Scorable scorer) {}

	@Override
	public void close() {
		if (this.ords instanceof SafeCloseable closeable) {
			closeable.close();
		}
		if (this.readerGen instanceof SafeCloseable closeable) {
			closeable.close();
		}
		if (this.values instanceof SafeCloseable closeable) {
			closeable.close();
		}
	}
}