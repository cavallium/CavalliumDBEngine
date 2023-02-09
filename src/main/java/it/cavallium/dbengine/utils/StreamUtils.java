package it.cavallium.dbengine.utils;

import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
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
