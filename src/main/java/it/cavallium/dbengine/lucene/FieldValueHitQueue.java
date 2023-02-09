package it.cavallium.dbengine.lucene;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SortField;

public interface FieldValueHitQueue {

	FieldComparator<?>[] getComparators();

	int[] getReverseMul();

	LeafFieldComparator[] getComparators(LeafReaderContext context);

	LLFieldDoc fillFields(LLSlotDoc entry);

	SortField[] getFields();
}
