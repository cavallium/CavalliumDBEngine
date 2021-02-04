package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLSort;
import java.util.Comparator;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public class MultiSort<T> {

	private final LLSort querySort;
	private final Comparator<T> resultSort;

	public MultiSort(LLSort querySort, Comparator<T> resultSort) {
		this.querySort = querySort;
		this.resultSort = resultSort;
	}

	/**
	 * Sort a lucene field and the results by a numeric sort field and an int value
	 * @param fieldName Lucene SortedNumericSortField field name
	 * @param toIntFunction function to retrieve the integer value of each result
	 * @param reverse descending sort
	 * @param <T> result type
	 * @return MultiSort object
	 */
	public static <T> MultiSort<T> sortedNumericInt(String fieldName, ToIntFunction<T> toIntFunction, boolean reverse) {
		// Create lucene sort
		LLSort querySort = LLSort.newSortedNumericSortField(fieldName, reverse);

		// Create result sort
		Comparator<T> resultSort = Comparator.comparingInt(toIntFunction);
		if (reverse) {
			resultSort = resultSort.reversed();
		}

		// Return the multi sort
		return new MultiSort<>(querySort, resultSort);
	}

	/**
	 * Sort a lucene field and the results by a numeric sort field and an long value
	 * @param fieldName Lucene SortedNumericSortField field name
	 * @param toLongFunction function to retrieve the long value of each result
	 * @param reverse descending sort
	 * @param <T> result type
	 * @return MultiSort object
	 */
	public static <T> MultiSort<T> sortedNumericLong(String fieldName, ToLongFunction<T> toLongFunction, boolean reverse) {
		// Create lucene sort
		LLSort querySort = LLSort.newSortedNumericSortField(fieldName, reverse);

		// Create result sort
		Comparator<T> resultSort = Comparator.comparingLong(toLongFunction);
		if (!reverse) {
			resultSort = resultSort.reversed();
		}

		// Return the multi sort
		return new MultiSort<>(querySort, resultSort);
	}

	public static <T> MultiSort<T> randomSortField() {
		return new MultiSort<>(LLSort.newRandomSortField(), (a, b) -> 0);
	}

	public static MultiSort<LLKeyScore> topScoreRaw() {
		return new MultiSort<>(LLSort.newSortScore(), Comparator.comparingDouble(LLKeyScore::getScore).reversed());
	}

	public static <T> MultiSort<SearchResultKey<T>> topScore() {
		return new MultiSort<>(LLSort.newSortScore(), Comparator.<SearchResultKey<T>>comparingDouble(SearchResultKey::getScore).reversed());
	}

	public static <T, U> MultiSort<SearchResultItem<T, U>> topScoreWithValues() {
		return new MultiSort<>(LLSort.newSortScore(), Comparator.<SearchResultItem<T, U>>comparingDouble(SearchResultItem::getScore).reversed());
	}

	public LLSort getQuerySort() {
		return querySort;
	}

	public Comparator<T> getResultSort() {
		return resultSort;
	}
}
