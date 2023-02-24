package it.cavallium.dbengine.utils;

import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import it.cavallium.dbengine.utils.PartitionByIntSpliterator.IntPartition;
import it.cavallium.dbengine.utils.PartitionBySpliterator.Partition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class StreamUtils {

	public static final ForkJoinPool LUCENE_SCHEDULER = new ForkJoinPool();
	public static final ForkJoinPool ROCKSDB_SCHEDULER = new ForkJoinPool();

	private static final Collector<?, ?, ?> TO_LIST_FAKE_COLLECTOR = new FakeCollector();
	private static final Collector<?, ?, ?> COUNT_FAKE_COLLECTOR = new FakeCollector();

	private static final Set<Collector.Characteristics> CH_NOID = Collections.emptySet();
	private static final Set<Characteristics> CH_CONCURRENT_NOID = Collections.unmodifiableSet(EnumSet.of(
			Characteristics.CONCURRENT,
			Characteristics.UNORDERED
	));
	private static final Object NULL = new Object();
	private static final Supplier<Object> NULL_SUPPLIER = () -> NULL;
	private static final BinaryOperator<Object> COMBINER = (a, b) -> NULL;
	private static final Function<Object, Void> FINISHER = x -> null;
	private static final Collector<Long,?, Long> SUMMING_LONG_COLLECTOR = new SummingLongCollector();

	public static <T> Collector<T, ?, @NotNull List<T>> fastListing() {
		//noinspection unchecked
		return (Collector<T, ?, List<T>>) TO_LIST_FAKE_COLLECTOR;
	}

	public static <T> Collector<T, ?, @NotNull Long> fastCounting() {
		//noinspection unchecked
		return (Collector<T, ?, Long>) COUNT_FAKE_COLLECTOR;
	}

	@SafeVarargs
	@SuppressWarnings("UnstableApiUsage")
	public static <X> Stream<X> mergeComparing(Comparator<? super X> comparator, Stream<X>... streams) {
		List<Iterator<X>> iterators = new ArrayList<>(streams.length);
		for (Stream<X> stream : streams) {
			var it = stream.iterator();
			if (it.hasNext()) {
				iterators.add(it);
			}
		}

		Stream<X> resultStream;

		if (iterators.isEmpty()) {
			resultStream = Stream.empty();
		} else if (iterators.size() == 1) {
			resultStream = Streams.stream(iterators.get(0));
		} else {
			resultStream = Streams.stream(Iterators.mergeSorted(iterators, comparator));
		}

		return resultStream.onClose(() -> {
			for (Stream<X> stream : streams) {
				stream.close();
			}
		});
	}

	public static <T> Stream<List<T>> batches(Stream<T> stream, int batchSize) {
		if (batchSize <= 0) {
			return Stream.of(toList(stream));
		} else if (batchSize == 1) {
			return stream.map(Collections::singletonList);
		} else {
			return StreamSupport
					.stream(new BatchSpliterator<>(stream.spliterator(), batchSize), stream.isParallel())
					.onClose(stream::close);
		}
	}

	@SuppressWarnings("UnstableApiUsage")
	public static <X> Stream<X> streamWhileNonNull(Supplier<? extends X> supplier) {
		var it = new Iterator<X>() {

			private boolean nextSet = false;
			private X next;

			@Override
			public boolean hasNext() {
				if (!nextSet) {
					next = supplier.get();
					nextSet = true;
				}
				return next != null;
			}

			@Override
			public X next() {
				nextSet = false;
				return next;
			}
		};
		return Streams.stream(it);
	}

	public static <X> List<X> toList(Stream<X> stream) {
		return collect(stream, fastListing());
	}

	@SuppressWarnings("DataFlowIssue")
	public static <X> long count(Stream<X> stream) {
		return collect(stream, fastCounting());
	}

	public static <X> List<X> toListOn(ForkJoinPool forkJoinPool, Stream<X> stream) {
		return collectOn(forkJoinPool, stream, fastListing());
	}

	public static <X> long countOn(ForkJoinPool forkJoinPool, Stream<X> stream) {
		return collectOn(forkJoinPool, stream, fastCounting());
	}

	/**
	 * Collects and closes the stream on the specified pool
	 */
	public static <I, X, R> R collectOn(@Nullable ForkJoinPool pool,
			@Nullable Stream<I> stream,
			@NotNull Collector<I, X, R> collector) {
		if (stream == null) {
			return null;
		}
		if (pool != null) {
			try {
				return pool.submit(() -> collect(stream.parallel(), collector)).get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
		} else {
			return collect(stream, collector);
		}
	}

	/**
	 * Collects and closes the stream on the specified pool
	 */
	@SuppressWarnings("unchecked")
	public static <I, R> R collect(@Nullable Stream<I> stream, @NotNull Collector<? super I, ?, R> collector) {
		try (stream) {
			if (collector == TO_LIST_FAKE_COLLECTOR) {
				if (stream != null) {
					return (R) stream.toList();
				} else {
					return (R) List.of();
				}
			} else if (collector == COUNT_FAKE_COLLECTOR) {
				if (stream != null) {
					return (R) (Long) stream.count();
				} else {
					return (R) (Long) 0L;
				}
			} else if (stream == null) {
				throw new NullPointerException("Stream is null");
			} else if (collector == SUMMING_LONG_COLLECTOR) {
				LongAdder sum = new LongAdder();
				((Stream<Long>) stream).forEach(sum::add);
				return (R) (Long) sum.sum();
			} else if (collector.getClass() == ExecutingCollector.class) {
				stream.forEach(((ExecutingCollector<? super I>) collector).getConsumer());
				return null;
			} else if (collector.getClass() == IteratingCollector.class) {
				stream.forEachOrdered(((IteratingCollector<? super I>) collector).getConsumer());
				return null;
			} else {
				return stream.collect(collector);
			}
		}
	}

	public static <I> Collector<I, ?, Void> executing(Consumer<? super I> consumer) {
		return new ExecutingCollector<>(consumer);
	}

	public static <I> Collector<I, ?, Void> iterating(Consumer<? super I> consumer) {
		return new IteratingCollector<>(consumer);
	}

	/**
	 * @param op must be fast and non-blocking!
	 */
	public static <T> Collector<T, ?, T> fastReducing(T identity, BinaryOperator<T> op) {
		return new ConcurrentUnorderedReducingCollector<>(identity, Function.identity(), op);
	}

	/**
	 * @param mapper must be fast and non-blocking!
	 * @param op must be fast and non-blocking!
	 */
	public static <T, U> Collector<T, ?, U> fastReducing(T identity, Function<? super T, U> mapper, BinaryOperator<U> op) {
		return new ConcurrentUnorderedReducingCollector<>(identity, mapper, op);
	}

	public static <K, E> Stream<Partition<K, E>> partitionBy(Function<? super E, K> partitionBy, Stream<E> in) {
		return StreamSupport
				.stream(new PartitionBySpliterator<>(in.spliterator(), partitionBy), in.isParallel())
				.onClose(in::close);
	}

	public static <E> Stream<IntPartition<E>> partitionByInt(ToIntFunction<? super E> partitionBy, Stream<E> in) {
		return StreamSupport
				.stream(new PartitionByIntSpliterator<>(in.spliterator(), partitionBy), in.isParallel())
				.onClose(in::close);
	}

	public static Collector<Long, ?, Long> fastSummingLong() {
		return SUMMING_LONG_COLLECTOR;
	}

	private record BatchSpliterator<E>(Spliterator<E> base, int batchSize) implements Spliterator<List<E>> {

		@Override
			public boolean tryAdvance(Consumer<? super List<E>> action) {
				final List<E> batch = new ArrayList<>(batchSize);
			//noinspection StatementWithEmptyBody
			for (int i = 0; i < batchSize && base.tryAdvance(batch::add); i++) {

			}
			if (batch.isEmpty()) {
				return false;
			}
			action.accept(batch);
			return true;
		}

		@Override
			public Spliterator<List<E>> trySplit() {
				if (base.estimateSize() <= batchSize) {
					return null;
				}
				final Spliterator<E> splitBase = this.base.trySplit();
				return splitBase == null ? null : new BatchSpliterator<>(splitBase, batchSize);
			}

		@Override
			public long estimateSize() {
				final double baseSize = base.estimateSize();
				return baseSize == 0 ? 0 : (long) Math.ceil(baseSize / (double) batchSize);
			}

		@Override
			public int characteristics() {
				return base.characteristics();
			}

	}

	private static final class FakeCollector implements Collector<Object, Object, Object> {

		@Override
		public Supplier<Object> supplier() {
			throw new IllegalStateException();
		}

		@Override
		public BiConsumer<Object, Object> accumulator() {
			throw new IllegalStateException();
		}

		@Override
		public BinaryOperator<Object> combiner() {
			throw new IllegalStateException();
		}

		@Override
		public Function<Object, Object> finisher() {
			throw new IllegalStateException();
		}

		@Override
		public Set<Characteristics> characteristics() {
			throw new IllegalStateException();
		}
	}

	private abstract static sealed class AbstractExecutingCollector<I> implements Collector<I, Object, Void> {

		private final Consumer<? super I> consumer;

		public AbstractExecutingCollector(Consumer<? super I> consumer) {
			this.consumer = consumer;
		}

		@Override
		public Supplier<Object> supplier() {
			return NULL_SUPPLIER;
		}

		@Override
		public BiConsumer<Object, I> accumulator() {
			return (o, i) -> consumer.accept(i);
		}

		@Override
		public BinaryOperator<Object> combiner() {
			return COMBINER;
		}

		@Override
		public Function<Object, Void> finisher() {
			return FINISHER;
		}

		public Consumer<? super I> getConsumer() {
			return consumer;
		}
	}

	private static final class ExecutingCollector<I> extends AbstractExecutingCollector<I> {

		public ExecutingCollector(Consumer<? super I> consumer) {
			super(consumer);
		}

		@Override
		public Set<Characteristics> characteristics() {
			return CH_CONCURRENT_NOID;
		}
	}

	private static final class IteratingCollector<I> extends AbstractExecutingCollector<I> {

		public IteratingCollector(Consumer<? super I> consumer) {
			super(consumer);
		}

		@Override
		public Set<Characteristics> characteristics() {
			return CH_NOID;
		}
	}

	private record ConcurrentUnorderedReducingCollector<T, U>(T identity, Function<? super T, U> mapper,
																														BinaryOperator<U> op) implements
			Collector<T, AtomicReference<U>, U> {

		@Override
		public Supplier<AtomicReference<U>> supplier() {
			return () -> new AtomicReference<>(mapper.apply(identity));
		}

		// Can be called from multiple threads!
		@Override
		public BiConsumer<AtomicReference<U>, T> accumulator() {
			return (a, t) -> a.updateAndGet(v1 -> op.apply(v1, mapper.apply(t)));
		}

		@Override
		public BinaryOperator<AtomicReference<U>> combiner() {
			return (a, b) -> {
				a.set(op.apply(a.get(), b.get()));
				return a;
			};
		}

		@Override
		public Function<AtomicReference<U>, U> finisher() {
			return AtomicReference::get;
		}

		@Override
		public Set<Characteristics> characteristics() {
			return CH_CONCURRENT_NOID;
		}
	}

	private static final class SummingLongCollector implements Collector<Long, LongAdder, Long> {

		public SummingLongCollector() {
		}

		@Override
		public Supplier<LongAdder> supplier() {
			return LongAdder::new;
		}

		@Override
		public BiConsumer<LongAdder, Long> accumulator() {
			return LongAdder::add;
		}

		@Override
		public BinaryOperator<LongAdder> combiner() {
			return (la1, la2) -> {
				la1.add(la2.sum());
				return la1;
			};
		}

		@Override
		public Function<LongAdder, Long> finisher() {
			return LongAdder::sum;
		}

		@Override
		public Set<Characteristics> characteristics() {
			return CH_CONCURRENT_NOID;
		}
	}
}
