package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.current.data.NumericSort;
import it.cavallium.dbengine.client.query.current.data.RandomSort;
import it.cavallium.dbengine.client.query.current.data.ScoreSort;
import it.cavallium.dbengine.client.query.current.data.Sort;
import it.cavallium.dbengine.database.LLKeyScore;
import java.util.Comparator;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public class MultiSort<T> {

	private final Sort querySort;
	private final Comparator<T> resultSort;

	public MultiSort(Sort querySort, Comparator<T> resultSort) {
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
		Sort querySort = NumericSort.of(fieldName, reverse);

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
		Sort querySort = NumericSort.of(fieldName, reverse);

		// Create result sort
		Comparator<T> resultSort = Comparator.comparingLong(toLongFunction);
		if (!reverse) {
			resultSort = resultSort.reversed();
		}

		// Return the multi sort
		return new MultiSort<>(querySort, resultSort);
	}

	public static <T> MultiSort<T> randomSortField() {
		return new MultiSort<>(RandomSort.of(), (a, b) -> 0);
	}

	public static MultiSort<LLKeyScore> topScoreRaw() {
		return new MultiSort<>(ScoreSort.of(), Comparator.comparingDouble(LLKeyScore::getScore).reversed());
	}

	public static <T> MultiSort<SearchResultKey<T>> topScore() {
		return new MultiSort<>(ScoreSort.of(), Comparator.<SearchResultKey<T>>comparingDouble(SearchResultKey::getScore).reversed());
	}

	public static <T, U> MultiSort<SearchResultItem<T, U>> topScoreWithValues() {
		return new MultiSort<>(ScoreSort.of(), Comparator.<SearchResultItem<T, U>>comparingDouble(SearchResultItem::getScore).reversed());
	}

	public Sort getQuerySort() {
		return querySort;
	}

	public Comparator<T> getResultSort() {
		return resultSort;
	}
}
