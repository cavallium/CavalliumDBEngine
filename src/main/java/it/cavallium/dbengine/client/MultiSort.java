package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.query.BasicType;
import it.cavallium.dbengine.client.query.current.data.DocSort;
import it.cavallium.dbengine.client.query.current.data.NoSort;
import it.cavallium.dbengine.client.query.current.data.NumericSort;
import it.cavallium.dbengine.client.query.current.data.RandomSort;
import it.cavallium.dbengine.client.query.current.data.ScoreSort;
import it.cavallium.dbengine.client.query.current.data.Sort;
import it.cavallium.dbengine.database.LLKeyScore;
import java.util.Comparator;
import java.util.Objects;
import java.util.StringJoiner;
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

	public boolean isSorted() {
		return querySort.getBasicType$() != BasicType.NoSort;
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

	public static <T> MultiSort<SearchResultKey<T>> noSort() {
		return new MultiSort<>(NoSort.of());
	}

	public static <T> MultiSort<SearchResultKey<T>> docSort() {
		return new MultiSort<>(DocSort.of());
	}

	public static <T> MultiSort<SearchResultKey<T>> numericSort(String field, boolean reverse) {
		return new MultiSort<>(NumericSort.of(field, reverse));
	}

	public static <T, U> MultiSort<SearchResultItem<T, U>> topScoreWithValues() {
		return new MultiSort<>(ScoreSort.of());
	}

	public static <T, U> MultiSort<SearchResultItem<T, U>> noScoreNoSortWithValues() {
		return new MultiSort<>(NoSort.of());
	}

	public Sort getQuerySort() {
		return querySort;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MultiSort<?> multiSort = (MultiSort<?>) o;
		return Objects.equals(querySort, multiSort.querySort);
	}

	@Override
	public int hashCode() {
		return Objects.hash(querySort);
	}

	@Override
	public String toString() {
		return querySort.toString();
	}
}
