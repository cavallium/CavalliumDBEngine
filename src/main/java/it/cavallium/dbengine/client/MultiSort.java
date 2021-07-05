package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.current.data.NumericSort;
import it.cavallium.dbengine.client.query.current.data.RandomSort;
import it.cavallium.dbengine.client.query.current.data.ScoreSort;
import it.cavallium.dbengine.client.query.current.data.Sort;
import it.cavallium.dbengine.database.LLKeyScore;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

public class MultiSort<T> {

	private final Sort querySort;

	public MultiSort(Sort querySort) {
		this.querySort = querySort;
	}

	/**
	 * Sort a lucene field and the results by a numeric sort field and an int value
	 * @param fieldName Lucene SortedNumericSortField field name
	 * @param reverse descending sort
	 * @param <T> result type
	 * @return MultiSort object
	 */
	public static <T> MultiSort<T> sortedNumericInt(String fieldName, boolean reverse) {
		// Create lucene sort
		Sort querySort = NumericSort.of(fieldName, reverse);

		// Return the multi sort
		return new MultiSort<>(querySort);
	}

	/**
	 * Sort a lucene field and the results by a numeric sort field and an long value
	 * @param fieldName Lucene SortedNumericSortField field name
	 * @param reverse descending sort
	 * @param <T> result type
	 * @return MultiSort object
	 */
	public static <T> MultiSort<T> sortedNumericLong(String fieldName, boolean reverse) {
		// Create lucene sort
		Sort querySort = NumericSort.of(fieldName, reverse);

		// Return the multi sort
		return new MultiSort<>(querySort);
	}

	public static <T> MultiSort<T> randomSortField() {
		return new MultiSort<>(RandomSort.of());
	}

	public static MultiSort<LLKeyScore> topScoreRaw() {
		return new MultiSort<>(ScoreSort.of());
	}

	public static <T> MultiSort<SearchResultKey<T>> topScore() {
		return new MultiSort<>(ScoreSort.of());
	}

	public static <T, U> MultiSort<SearchResultItem<T, U>> topScoreWithValues() {
		return new MultiSort<>(ScoreSort.of());
	}

	public Sort getQuerySort() {
		return querySort;
	}
}
