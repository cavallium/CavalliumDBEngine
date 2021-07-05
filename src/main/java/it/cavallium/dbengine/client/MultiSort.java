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

public class MultiSort<T, U> {

	private final Sort querySort;
	@NotNull
	private final Function<T, Mono<U>> transformer;
	private final Comparator<U> resultSort;

	public MultiSort(Sort querySort, Function<T, Mono<U>> transformer, Comparator<U> resultSort) {
		this.querySort = querySort;
		this.transformer = transformer;
		this.resultSort = resultSort;
	}

	/**
	 * Sort a lucene field and the results by a numeric sort field and an int value
	 * @param fieldName Lucene SortedNumericSortField field name
	 * @param transformer Transform a value to a comparable value asynchronously
	 * @param toIntFunction function to retrieve the integer value of each result
	 * @param reverse descending sort
	 * @param <T> result type
	 * @return MultiSort object
	 */
	public static <T, U> MultiSort<T, U> sortedNumericInt(String fieldName,
			Function<T, Mono<U>> transformer,
			ToIntFunction<U> toIntFunction,
			boolean reverse) {
		// Create lucene sort
		Sort querySort = NumericSort.of(fieldName, reverse);

		// Create result sort
		Comparator<U> resultSort = Comparator.comparingInt(toIntFunction);
		if (reverse) {
			resultSort = resultSort.reversed();
		}

		// Return the multi sort
		return new MultiSort<>(querySort, transformer, resultSort);
	}

	/**
	 * Sort a lucene field and the results by a numeric sort field and an long value
	 * @param fieldName Lucene SortedNumericSortField field name
	 * @param transformer Transform a value to a comparable value asynchronously
	 * @param toLongFunction function to retrieve the long value of each result
	 * @param reverse descending sort
	 * @param <T> result type
	 * @return MultiSort object
	 */
	public static <T, U> MultiSort<T, U> sortedNumericLong(String fieldName,
			Function<T, Mono<U>> transformer,
			ToLongFunction<U> toLongFunction,
			boolean reverse) {
		// Create lucene sort
		Sort querySort = NumericSort.of(fieldName, reverse);

		// Create result sort
		Comparator<U> resultSort = Comparator.comparingLong(toLongFunction);
		if (!reverse) {
			resultSort = resultSort.reversed();
		}

		// Return the multi sort
		return new MultiSort<>(querySort, transformer, resultSort);
	}

	public static <T> MultiSort<T, T> randomSortField() {
		return new MultiSort<>(RandomSort.of(), Mono::just, (a, b) -> 0);
	}

	public static MultiSort<LLKeyScore, LLKeyScore> topScoreRaw() {
		Comparator<LLKeyScore> comp = Comparator.comparingDouble(LLKeyScore::score).reversed();
		return new MultiSort<>(ScoreSort.of(), Mono::just, comp);
	}

	public static <T> MultiSort<SearchResultKey<T>, SearchResultKey<T>> topScore() {
		return new MultiSort<>(ScoreSort.of(), Mono::just, Comparator.<SearchResultKey<T>>comparingDouble(SearchResultKey::score).reversed());
	}

	public static <T, U> MultiSort<SearchResultItem<T, U>, SearchResultItem<T, U>> topScoreWithValues() {
		return new MultiSort<>(ScoreSort.of(), Mono::just, Comparator.<SearchResultItem<T, U>>comparingDouble(SearchResultItem::score).reversed());
	}

	public Sort getQuerySort() {
		return querySort;
	}

	@NotNull
	public Function<T, Mono<U>> getTransformer() {
		return transformer;
	}

	public Comparator<U> getResultSort() {
		return resultSort;
	}
}
