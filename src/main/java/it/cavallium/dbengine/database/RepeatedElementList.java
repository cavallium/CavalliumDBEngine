package it.cavallium.dbengine.database;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("ClassCanBeRecord")
public class RepeatedElementList<T> implements List<T> {

	private final T element;
	private final int size;

	public RepeatedElementList(T element, int size) {
		this.element = element;
		this.size = size;
	}

	private UnsupportedOperationException uoe() {
		return new UnsupportedOperationException();
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	@Override
	public boolean contains(Object o) {
		return Objects.equals(element, o);
	}

	@NotNull
	@Override
	public Iterator<T> iterator() {
		return this.listIterator();
	}

	@NotNull
	@Override
	public Object @NotNull [] toArray() {
		var arr = new Object[size];
		Arrays.fill(arr, element);
		return arr;
	}

	@NotNull
	@Override
	public <T1> T1 @NotNull [] toArray(@NotNull T1 @NotNull [] a) {
		var arr = Arrays.copyOf(a, size);
		Arrays.fill(arr, element);
		return arr;
	}

	@Override
	public boolean add(T t) {
		throw uoe();
	}

	@Override
	public boolean remove(Object o) {
		throw uoe();
	}

	@Override
	public boolean containsAll(@NotNull Collection<?> c) {
		for (Object o : c) {
			if (!contains(o)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean addAll(@NotNull Collection<? extends T> c) {
		throw uoe();
	}

	@Override
	public boolean addAll(int index, @NotNull Collection<? extends T> c) {
		throw uoe();
	}

	@Override
	public boolean removeAll(@NotNull Collection<?> c) {
		throw uoe();
	}

	@Override
	public boolean retainAll(@NotNull Collection<?> c) {
		throw uoe();
	}

	@Override
	public void clear() {
		throw uoe();
	}

	@Override
	public T get(int i) {
		if (i >= 0 && i < size) {
			return element;
		} else {
			throw new IndexOutOfBoundsException(i);
		}
	}

	@Override
	public T set(int index, T element) {
		throw uoe();
	}

	@Override
	public void add(int index, T element) {
		throw uoe();
	}

	@Override
	public T remove(int index) {
		throw uoe();
	}

	@Override
	public int indexOf(Object o) {
		if (contains(o)) {
			return 0;
		} else {
			return -1;
		}
	}

	@Override
	public int lastIndexOf(Object o) {
		return indexOf(o);
	}

	@NotNull
	@Override
	public ListIterator<T> listIterator() {
		return listIterator(0);
	}

	@NotNull
	@Override
	public ListIterator<T> listIterator(int index) {
		return new ListIterator<>() {
			int position = index - 1;

			@Override
			public boolean hasNext() {
				return position + 1 < size;
			}

			@Override
			public T next() {
				position++;
				return get(position);
			}

			@Override
			public boolean hasPrevious() {
				return position - 1 >= 0;
			}

			@Override
			public T previous() {
				position--;
				return get(position);
			}

			@Override
			public int nextIndex() {
				return position + 1;
			}

			@Override
			public int previousIndex() {
				return position - 1;
			}

			@Override
			public void remove() {
				throw uoe();
			}

			@Override
			public void set(T t) {
				throw uoe();
			}

			@Override
			public void add(T t) {
				throw uoe();
			}
		};
	}

	@NotNull
	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		return new RepeatedElementList<>(element, toIndex - fromIndex);
	}
}
