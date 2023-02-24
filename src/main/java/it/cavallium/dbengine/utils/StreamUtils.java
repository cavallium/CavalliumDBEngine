package it.cavallium.dbengine.utils;

import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import it.cavallium.dbengine.database.SubStageEntry;
import it.cavallium.dbengine.database.collections.DatabaseStage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtils {

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
		return batchSize <= 0
				? Stream.of(stream.collect(Collectors.toList()))
				: StreamSupport.stream(new BatchSpliterator<>(stream.spliterator(), batchSize), stream.isParallel());
	}

	@SuppressWarnings("UnstableApiUsage")
	public static <X> Stream<X> streamWhileNonNull(Supplier<X> supplier) {
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

	public static <X> List<X> toListClose(Stream<X> stream) {
		try (stream) {
			return stream.toList();
		}
	}

	public static <T, R, A> R collectClose(Stream<T> stream, Collector<? super T, A, R> collector) {
		try (stream) {
			return stream.collect(collector);
		}
	}

	public static <X> long countClose(Stream<X> stream) {
		try (stream) {
			return stream.count();
		}
	}

	public static <X> X scheduleOnPool(ForkJoinPool pool, Callable<X> supplier) {
		try {
			return pool.submit(supplier).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
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
}
