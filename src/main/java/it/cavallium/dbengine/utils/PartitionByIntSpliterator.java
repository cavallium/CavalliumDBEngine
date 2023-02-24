package it.cavallium.dbengine.utils;

import static java.util.Comparator.naturalOrder;

import it.cavallium.dbengine.utils.PartitionByIntSpliterator.IntPartition;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/**
 * Create a partitioned view of an existing spliterator
 *
 * @author cavallium
 */
public class PartitionByIntSpliterator<E> extends AbstractSpliterator<IntPartition<E>> {

	private final Spliterator<E> spliterator;
	private final ToIntFunction<? super E> partitionBy;
	private HoldingConsumer<E> holder;
	private Comparator<IntPartition<E>> comparator;

	public PartitionByIntSpliterator(Spliterator<E> toWrap, ToIntFunction<? super E> partitionBy) {
		super(Long.MAX_VALUE, toWrap.characteristics() & ~SIZED | NONNULL);
		this.spliterator = toWrap;
		this.partitionBy = partitionBy;
	}

	@Override
	public boolean tryAdvance(Consumer<? super IntPartition<E>> action) {
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
		final int partitionKey = partitionBy.applyAsInt(h.value);
		boolean didAdvance;
		do {
			partition.add(h.value);
		} while ((didAdvance = spliterator.tryAdvance(h)) && partitionBy.applyAsInt(h.value) == partitionKey);
		if (!didAdvance) {
			holder = null;
		}
		action.accept(new IntPartition<>(partitionKey, partition));
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
	public Comparator<? super IntPartition<E>> getComparator() {
		final Comparator<IntPartition<E>> c = this.comparator;
		return c != null ? c : (this.comparator = comparator());
	}

	private Comparator<IntPartition<E>> comparator() {
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

	public record IntPartition<V>(int key, List<V> values) {}
}