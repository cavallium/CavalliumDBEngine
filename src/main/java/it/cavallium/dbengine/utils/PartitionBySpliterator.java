package it.cavallium.dbengine.utils;

import it.cavallium.dbengine.utils.PartitionBySpliterator.Partition;
import java.util.*;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Comparator.naturalOrder;

/**
 * Create a partitioned view of an existing spliterator
 *
 * @author cavallium
 */
public class PartitionBySpliterator<K, E> extends AbstractSpliterator<Partition<K, E>> {

	private final Spliterator<E> spliterator;
	private final Function<? super E, K> partitionBy;
	private HoldingConsumer<E> holder;
	private Comparator<Partition<K, E>> comparator;

	public PartitionBySpliterator(Spliterator<E> toWrap, Function<? super E, K> partitionBy) {
		super(Long.MAX_VALUE, toWrap.characteristics() & ~SIZED | NONNULL);
		this.spliterator = toWrap;
		this.partitionBy = partitionBy;
	}

	@Override
	public boolean tryAdvance(Consumer<? super Partition<K, E>> action) {
		final HoldingConsumer<E> h;
		if (holder == null) {
			h = new HoldingConsumer<>();
			if (!spliterator.tryAdvance(h)) {
				return false;
			}
			holder = h;
		} else {
			h = holder;
		}
		final ArrayList<E> partition = new ArrayList<>();
		final K partitionKey = partitionBy.apply(h.value);
		boolean didAdvance;
		do {
			partition.add(h.value);
		} while ((didAdvance = spliterator.tryAdvance(h)) && Objects.equals(partitionBy.apply(h.value), partitionKey));
		if (!didAdvance) {
			holder = null;
		}
		action.accept(new Partition<>(partitionKey, partition));
		return true;
	}

	static final class HoldingConsumer<T> implements Consumer<T> {

		T value;

		@Override
		public void accept(T value) {
			this.value = value;
		}
	}

	@Override
	public Comparator<? super Partition<K, E>> getComparator() {
		final Comparator<Partition<K, E>> c = this.comparator;
		return c != null ? c : (this.comparator = comparator());
	}

	private Comparator<Partition<K, E>> comparator() {
		final Comparator<? super E> cmp;
		{
			var comparator = spliterator.getComparator();
			//noinspection unchecked
			cmp = comparator != null ? comparator : (Comparator<? super E>) naturalOrder();
		}
		return (left, right) -> {
			var leftV = left.values;
			var rightV = right.values;
			final int c = cmp.compare(leftV.get(0), rightV.get(0));
			return c != 0 ? c : cmp.compare(leftV.get(leftV.size() - 1), rightV.get(rightV.size() - 1));
		};
	}

	public record Partition<K, V>(K key, List<V> values) {}
}