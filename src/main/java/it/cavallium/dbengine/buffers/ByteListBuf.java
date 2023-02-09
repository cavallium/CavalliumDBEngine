package it.cavallium.dbengine.buffers;

import it.unimi.dsi.fastutil.bytes.AbstractByteList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteCollection;
import it.unimi.dsi.fastutil.bytes.ByteConsumer;
import it.unimi.dsi.fastutil.bytes.ByteIterator;
import it.unimi.dsi.fastutil.bytes.ByteIterators;
import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.bytes.ByteListIterator;
import it.unimi.dsi.fastutil.bytes.ByteSpliterator;
import it.unimi.dsi.fastutil.bytes.ByteSpliterators;
import java.io.Serial;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.stream.SafeByteArrayInputStream;
import org.warp.commonutils.stream.SafeByteArrayOutputStream;
import org.warp.commonutils.stream.SafeDataOutput;

class ByteListBuf extends ByteArrayList implements Buf {

	private boolean mutable = true;

	protected ByteListBuf(byte[] a, boolean wrapped) {
		super(a, wrapped);
	}

	public ByteListBuf(int capacity) {
		super(capacity);
	}

	public ByteListBuf() {
	}

	public ByteListBuf(Collection<? extends Byte> c) {
		super(c);
	}

	public ByteListBuf(ByteCollection c) {
		super(c);
	}

	public ByteListBuf(ByteList l) {
		super(l);
	}

	public ByteListBuf(byte[] a) {
		super(a);
	}

	public ByteListBuf(byte[] a, int offset, int length) {
		super(a, offset, length);
	}

	public ByteListBuf(Iterator<? extends Byte> i) {
		super(i);
	}

	public ByteListBuf(ByteIterator i) {
		super(i);
	}

	/**
	 * Wraps a given array into an array list of given size.
	 *
	 * <p>
	 * Note it is guaranteed that the type of the array returned by {@link #elements()} will be the same
	 * (see the comments in the class documentation).
	 *
	 * @param a an array to wrap.
	 * @param length the length of the resulting array list.
	 * @return a new array list of the given size, wrapping the given array.
	 */
	public static ByteListBuf wrap(final byte[] a, final int length) {
		if (length > a.length) throw new IllegalArgumentException("The specified length (" + length + ") is greater than the array size (" + a.length + ")");
		final ByteListBuf l = new ByteListBuf(a, true);
		l.size = length;
		return l;
	}

	/**
	 * Wraps a given array into an array list.
	 *
	 * <p>
	 * Note it is guaranteed that the type of the array returned by {@link #elements()} will be the same
	 * (see the comments in the class documentation).
	 *
	 * @param a an array to wrap.
	 * @return a new array list wrapping the given array.
	 */
	public static ByteListBuf wrap(final byte[] a) {
		return wrap(a, a.length);
	}

	/**
	 * Creates a new empty array list.
	 *
	 * @return a new empty array list.
	 */
	public static ByteListBuf of() {
		return new ByteListBuf();
	}

	/**
	 * Creates an array list using an array of elements.
	 *
	 * @param init a the array the will become the new backing array of the array list.
	 * @return a new array list backed by the given array.
	 * @see #wrap
	 */

	public static ByteListBuf of(final byte... init) {
		return wrap(init);
	}

	@Override
	public byte @NotNull [] asArray() {
		if (this.size() == a.length) {
			return this.a;
		} else {
			return this.toByteArray();
		}
	}

	@Override
	public byte @Nullable [] asArrayStrict() {
		if (this.size() == a.length) {
			return a;
		} else {
			return null;
		}
	}

	@Override
	public byte @Nullable [] asUnboundedArray() {
		return a;
	}

	@Override
	public byte @Nullable [] asUnboundedArrayStrict() {
		return a;
	}

	@Override
	public boolean isMutable() {
		return mutable;
	}

	@Override
	public void freeze() {
		mutable = false;
	}

	@Override
	public Buf subList(int from, int to) {
		if (from == 0 && to == size()) return this;
		ensureIndex(from);
		ensureIndex(to);
		if (from > to) throw new IndexOutOfBoundsException("Start index (" + from + ") is greater than end index (" + to + ")");
		return new SubList(from, to);
	}

	@Override
	public Buf copy() {
		var copied = ByteListBuf.wrap(this.a.clone());
		copied.size = this.size;
		return copied;
	}

	@Override
	public SafeByteArrayInputStream binaryInputStream() {
		return new SafeByteArrayInputStream(this.a, 0, this.size);
	}

	@Override
	public void writeTo(SafeDataOutput dataOutput) {
		dataOutput.write(this.a, 0, this.size);
	}

	@Override
	public SafeByteArrayOutputStream binaryOutputStream(int from, int to) {
		it.unimi.dsi.fastutil.Arrays.ensureFromTo(size, from, to);
		return new SafeByteArrayOutputStream(a, from, to);
	}

	@Override
	public boolean equals(int aStartIndex, Buf b, int bStartIndex, int length) {
		return b.equals(bStartIndex, this.a, aStartIndex, length);
	}

	@Override
	public boolean equals(int aStartIndex, byte[] b, int bStartIndex, int length) {
		if (aStartIndex < 0) return false;
		if (aStartIndex + length > this.size) {
			return false;
		}
		return Arrays.equals(a, aStartIndex, aStartIndex + length, b, bStartIndex, bStartIndex + length);
	}

	@Override
	public String toString(Charset charset) {
		return new String(a, 0, size, charset);
	}

	private class SubList extends AbstractByteList.ByteRandomAccessSubList implements Buf {
		@Serial
		private static final long serialVersionUID = -3185226345314976296L;

		private boolean subMutable = true;

		protected SubList(int from, int to) {
			super(ByteListBuf.this, from, to);
		}

		// Most of the inherited methods should be fine, but we can override a few of them for performance.
		// Needed because we can't access the parent class' instance variables directly in a different
		// instance of SubList.
		private byte[] getParentArray() {
			return a;
		}

		@Override
		public @NotNull Buf subList(int from, int to) {
			it.unimi.dsi.fastutil.Arrays.ensureFromTo(a.length, from, to);
			if (from > to) throw new IllegalArgumentException("Start index (" + from + ") is greater than end index (" + to + ")");
			// Sadly we have to rewrap this, because if there is a sublist of a sublist, and the
			// subsublist adds, both sublists need to update their "to" value.
			return new SubList(from, to);
		}

		@Override
		public Buf copy() {
			return Buf.wrap(Arrays.copyOfRange(a, from, to));
		}

		@Override
		public SafeByteArrayInputStream binaryInputStream() {
			return new SafeByteArrayInputStream(a, from, size());
		}

		@Override
		public void writeTo(SafeDataOutput dataOutput) {
			dataOutput.write(a, from, size());
		}

		@Override
		public SafeByteArrayOutputStream binaryOutputStream(int from, int to) {
			it.unimi.dsi.fastutil.Arrays.ensureFromTo(size(), from, to);
			return new SafeByteArrayOutputStream(a, from + this.from, to + this.from);
		}

		@Override
		public boolean equals(int aStartIndex, Buf b, int bStartIndex, int length) {
			return b.equals(bStartIndex, a, aStartIndex + from, length);
		}

		@Override
		public boolean equals(int aStartIndex, byte[] b, int bStartIndex, int length) {
			var aFrom = from + aStartIndex;
			var aTo = from + aStartIndex + length;
			if (aFrom < from) return false;
			if (aTo > to) return false;
			return Arrays.equals(a, aFrom, aTo, b, bStartIndex, bStartIndex + length);
		}

		@Override
		public byte getByte(int i) {
			ensureRestrictedIndex(i);
			return a[i + from];
		}

		@Override
		public byte @NotNull [] asArray() {
			if (this.from == 0 && this.to == a.length) {
				return a;
			} else {
				return toByteArray();
			}
		}

		@Override
		public byte @Nullable [] asArrayStrict() {
			if (this.from == 0 && this.to == a.length) {
				return a;
			} else {
				return null;
			}
		}

		@Override
		public byte @Nullable [] asUnboundedArray() {
			if (from == 0) {
				return a;
			} else {
				return toByteArray();
			}
		}

		@Override
		public byte @Nullable [] asUnboundedArrayStrict() {
			if (from == 0) {
				return a;
			} else {
				return null;
			}
		}

		@Override
		public boolean isMutable() {
			return mutable && subMutable;
		}

		@Override
		public void freeze() {
			subMutable = false;
		}

		private final class SubListIterator extends ByteIterators.AbstractIndexBasedListIterator {
			// We are using pos == 0 to be 0 relative to SubList.from (meaning you need to do a[from + i] when
			// accessing array).
			SubListIterator(int index) {
				super(0, index);
			}

			@Override
			protected byte get(int i) {
				return a[from + i];
			}

			@Override
			protected void add(int i, byte k) {
				ByteListBuf.SubList.this.add(i, k);
			}

			@Override
			protected void set(int i, byte k) {
				ByteListBuf.SubList.this.set(i, k);
			}

			@Override
			protected void remove(int i) {
				ByteListBuf.SubList.this.removeByte(i);
			}

			@Override
			protected int getMaxPos() {
				return to - from;
			}

			@Override
			public byte nextByte() {
				if (!hasNext()) throw new NoSuchElementException();
				return a[from + (lastReturned = pos++)];
			}

			@Override
			public byte previousByte() {
				if (!hasPrevious()) throw new NoSuchElementException();
				return a[from + (lastReturned = --pos)];
			}

			@Override
			public void forEachRemaining(final ByteConsumer action) {
				final int max = to - from;
				while (pos < max) {
					action.accept(a[from + (lastReturned = pos++)]);
				}
			}
		}

		@Override
		public @NotNull ByteListIterator listIterator(int index) {
			return new ByteListBuf.SubList.SubListIterator(index);
		}

		private final class SubListSpliterator extends ByteSpliterators.LateBindingSizeIndexBasedSpliterator {
			// We are using pos == 0 to be 0 relative to real array 0
			SubListSpliterator() {
				super(from);
			}

			private SubListSpliterator(int pos, int maxPos) {
				super(pos, maxPos);
			}

			@Override
			protected int getMaxPosFromBackingStore() {
				return to;
			}

			@Override
			protected byte get(int i) {
				return a[i];
			}

			@Override
			protected ByteListBuf.SubList.SubListSpliterator makeForSplit(int pos, int maxPos) {
				return new ByteListBuf.SubList.SubListSpliterator(pos, maxPos);
			}

			@Override
			public boolean tryAdvance(final ByteConsumer action) {
				if (pos >= getMaxPos()) return false;
				action.accept(a[pos++]);
				return true;
			}

			@Override
			public void forEachRemaining(final ByteConsumer action) {
				final int max = getMaxPos();
				while (pos < max) {
					action.accept(a[pos++]);
				}
			}
		}

		@Override
		public ByteSpliterator spliterator() {
			return new ByteListBuf.SubList.SubListSpliterator();
		}

		boolean contentsEquals(byte[] otherA, int otherAFrom, int otherATo) {
			if (a == otherA && from == otherAFrom && to == otherATo) return true;
			return Arrays.equals(a, from, to, otherA, otherAFrom, otherATo);
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) return true;
			if (o == null) return false;
			if (!(o instanceof java.util.List)) return false;
			if (o instanceof ByteListBuf other) {
				return contentsEquals(other.a, 0, other.size());
			}
			if (o instanceof SubList other) {
				return contentsEquals(other.getParentArray(), other.from, other.to);
			}
			return super.equals(o);
		}

		int contentsCompareTo(byte[] otherA, int otherAFrom, int otherATo) {
			if (a == otherA && from == otherAFrom && to == otherATo) return 0;
			return Arrays.compareUnsigned(a, from, to, otherA, otherAFrom, otherATo);
		}

		@Override
		public int compareTo(final java.util.@NotNull List<? extends Byte> l) {
			if (l instanceof ByteListBuf other) {
				return contentsCompareTo(other.a, 0, other.size());
			}
			if (l instanceof ByteListBuf.SubList other) {
				return contentsCompareTo(other.getParentArray(), other.from, other.to);
			}
			return super.compareTo(l);
		}

		@Override
		public String toString(Charset charset) {
			return new String(a, from, to, charset);
		}
	}
}
