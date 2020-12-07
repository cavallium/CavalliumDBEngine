package it.cavallium.dbengine.database.utils;

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
