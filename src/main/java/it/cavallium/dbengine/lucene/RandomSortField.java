package it.cavallium.dbengine.lucene;

import org.apache.lucene.search.SortField;

public class RandomSortField extends SortField {

	public RandomSortField() {
		super("", new RandomFieldComparatorSource());
	}

	@Override
	public boolean needsScores() {
		return false;
	}
}
