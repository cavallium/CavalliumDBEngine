package it.cavallium.dbengine.lucene;

import io.net5.buffer.ByteBuf;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.disk.LLTempLMDBEnv;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

public class LLSlotDocCodec implements LMDBSortedCodec<LLSlotDoc>, FieldValueHitQueue, SafeCloseable {

	private final SortField[] fields;

	protected final FieldComparator<?>[] comparators;
	protected final int[] reverseMul;

	public LLSlotDocCodec(LLTempLMDBEnv env, int numHits, SortField[] fields) {
		// When we get here, fields.length is guaranteed to be > 0, therefore no
		// need to check it again.

		// All these are required by this class's API - need to return arrays.
		// Therefore even in the case of a single comparator, create an array
		// anyway.
		this.fields = fields;
		int numComparators = fields.length;
		comparators = new FieldComparator<?>[numComparators];
		reverseMul = new int[numComparators];
		for (int i = 0; i < numComparators; ++i) {
			SortField field = fields[i];
			reverseMul[i] = field.getReverse() ? -1 : 1;
			comparators[i] = LMDBComparator.getComparator(env, field, numHits, i);
		}
	}

	@Override
	public ByteBuf serialize(Function<Integer, ByteBuf> allocator, LLSlotDoc data) {
		var buf = allocator.apply(Float.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES);
		setScore(buf, data.score());
		setDoc(buf, data.doc());
		setShardIndex(buf, data.shardIndex());
		setSlot(buf, data.slot());
		buf.writerIndex(Float.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES);
		return buf.asReadOnly();
	}

	@Override
	public LLSlotDoc deserialize(ByteBuf buf) {
		return new LLSlotDoc(getDoc(buf), getScore(buf), getShardIndex(buf), getSlot(buf));
	}

	@Override
	public int compare(LLSlotDoc hitA, LLSlotDoc hitB) {
		int numComparators = comparators.length;
		for (int i = 0; i < numComparators; ++i) {
			final int c = reverseMul[i] * comparators[i].compare(hitA.slot(), hitB.slot());
			if (c != 0) {
				// Short circuit
				return -c;
			}
		}

		// avoid random sort order that could lead to duplicates (bug #31241):
		return Integer.compare(hitB.doc(), hitA.doc());
	}

	@Override
	public int compareDirect(ByteBuf hitA, ByteBuf hitB) {

		assert hitA != hitB;
		assert getSlot(hitA) != getSlot(hitB);

		int numComparators = comparators.length;
		for (int i = 0; i < numComparators; ++i) {
			final int c = reverseMul[i] * comparators[i].compare(getSlot(hitA), getSlot(hitB));
			if (c != 0) {
				// Short circuit
				return -c;
			}
		}

		// avoid random sort order that could lead to duplicates (bug #31241):
		return Integer.compare(getDoc(hitB), getDoc(hitA));
	}

	private static float getScore(ByteBuf hit) {
		return hit.getFloat(0);
	}

	private static int getDoc(ByteBuf hit) {
		return hit.getInt(Float.BYTES);
	}

	private static int getShardIndex(ByteBuf hit) {
		return hit.getInt(Float.BYTES + Integer.BYTES);
	}

	private static int getSlot(ByteBuf hit) {
		return hit.getInt(Float.BYTES + Integer.BYTES + Integer.BYTES);
	}

	private static void setScore(ByteBuf hit, float score) {
		hit.setFloat(0, score);
	}

	private static void setDoc(ByteBuf hit, int doc) {
		hit.setInt(Float.BYTES, doc);
	}

	private static void setShardIndex(ByteBuf hit, int shardIndex) {
		hit.setInt(Float.BYTES + Integer.BYTES, shardIndex);
	}

	private static void setSlot(ByteBuf hit, int slot) {
		hit.setInt(Float.BYTES + Integer.BYTES + Integer.BYTES, slot);
	}

	@Override
	public FieldComparator<?>[] getComparators() {
		return comparators;
	}

	@Override
	public int[] getReverseMul() {
		return reverseMul;
	}

	@Override
	public LeafFieldComparator[] getComparators(LeafReaderContext context) throws IOException {
		LeafFieldComparator[] comparators = new LeafFieldComparator[this.comparators.length];
		for (int i = 0; i < comparators.length; ++i) {
			comparators[i] = this.comparators[i].getLeafComparator(context);
		}
		return comparators;
	}

	/**
	 * Given a queue Entry, creates a corresponding FieldDoc that contains the values used to sort the
	 * given document. These values are not the raw values out of the index, but the internal
	 * representation of them. This is so the given search hit can be collated by a MultiSearcher with
	 * other search hits.
	 *
	 * @param entry The Entry used to create a FieldDoc
	 * @return The newly created FieldDoc
	 * @see IndexSearcher#search(Query,int, Sort)
	 */
	@Override
	public LLFieldDoc fillFields(final LLSlotDoc entry) {
		final int n = comparators.length;
		final List<Object> fields = new ArrayList<>(n);
		for (FieldComparator<?> comparator : comparators) {
			fields.add(comparator.value(entry.slot()));
		}
		// if (maxscore > 1.0f) doc.score /= maxscore;   // normalize scores
		return new LLFieldDoc(entry.doc(), entry.score(), entry.shardIndex(), fields);
	}

	/** Returns the SortFields being used by this hit queue. */
	@Override
	public SortField[] getFields() {
		return fields;
	}

	@Override
	public void close() {
		for (FieldComparator<?> comparator : this.comparators) {
			if (comparator instanceof SafeCloseable closeable) {
				closeable.close();
			}
		}
	}
}
