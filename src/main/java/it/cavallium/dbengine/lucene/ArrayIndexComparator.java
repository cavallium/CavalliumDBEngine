package it.cavallium.dbengine.lucene;

import java.util.Comparator;
import org.apache.lucene.index.IndexReader;

public class ArrayIndexComparator implements Comparator<IndexReader> {

	private final Comparator<Object> comp;

	public ArrayIndexComparator(IndexReader[] indexReaders) {
		this.comp = Comparator.comparingInt(reader -> {
			for (int i = 0; i < indexReaders.length; i++) {
				if (indexReaders[i] == reader) {
					return i;
				}
			}
			throw new IllegalStateException();
		});
	}

	@Override
	public int compare(IndexReader o1, IndexReader o2) {
		return comp.compare(o1, o2);
	}
}
