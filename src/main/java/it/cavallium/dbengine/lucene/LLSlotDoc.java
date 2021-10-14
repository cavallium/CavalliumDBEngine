package it.cavallium.dbengine.lucene;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldValueHitQueue;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.search.ScoreDoc;

/** Extension of ScoreDoc to also store the {@link FieldComparator} slot. */
public record LLSlotDoc(int doc, float score, int shardIndex, int slot) implements LLDocElement {

	public ScoreDoc toScoreDoc() {
		return new ScoreDoc(doc, score, shardIndex);
	}

	public ScoreDoc toEntry() {
		var entry = new Entry(doc, slot);
		entry.shardIndex = shardIndex;
		return entry;
	}

	@Override
	public String toString() {
		return "slot:" + slot + " doc=" + doc + " score=" + score + " shardIndex=" + shardIndex;
	}
}
