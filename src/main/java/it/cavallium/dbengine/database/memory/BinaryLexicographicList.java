package it.cavallium.dbengine.database.memory;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteCollection;
import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.bytes.ByteListIterator;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("ClassCanBeRecord")
public class BinaryLexicographicList implements ByteList {

	private final byte[] bytes;

	public BinaryLexicographicList(byte[] bytes) {
		this.bytes = bytes;
	}

	@Override
	public int size() {
		return bytes.length;
	}

	@Override
	public boolean isEmpty() {
		return bytes.length == 0;
	}

	@Override
	public ByteListIterator iterator() {
		return ByteList.of(bytes).iterator();
	}

	@NotNull
	@Override
	public Object @NotNull [] toArray() {
		var output = new Object[bytes.length];
		for (int i = 0; i < bytes.length; i++) {
			output[i] = bytes[i];
		}
		return output;
	}

	@SuppressWarnings("unchecked")
	@NotNull
	@Override
	public <T> T @NotNull [] toArray(T @NotNull[] a) {
		Object[] content = toArray();
		if (a.length < bytes.length)
			// Make a new array of a's runtime type, but my contents:
			return (T[]) Arrays.copyOf(content, bytes.length, a.getClass());
		System.arraycopy(content, 0, a, 0, bytes.length);
		if (a.length > bytes.length)
			a[bytes.length] = null;
		return a;
	}

	@Override
	public boolean containsAll(@NotNull Collection<?> c) {
		return ByteArrayList.wrap(bytes).containsAll(c);
	}

	@Override
	public boolean addAll(@NotNull Collection<? extends Byte> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(int index, @NotNull Collection<? extends Byte> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(@NotNull Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(@NotNull Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ByteListIterator listIterator() {
		return ByteList.of(bytes).listIterator();
	}

	@Override
	public ByteListIterator listIterator(int index) {
		return ByteList.of(bytes).listIterator(index);
	}

	@Override
	public ByteList subList(int from, int to) {
		return ByteList.of(bytes).subList(from, to);
	}

	@Override
	public void size(int size) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void getElements(int from, byte[] a, int offset, int length) {
		ByteList.of(bytes).getElements(from, a, offset, length);
	}

	@Override
	public void removeElements(int from, int to) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addElements(int index, byte[] a) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addElements(int index, byte[] a, int offset, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean add(byte key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean contains(byte key) {
		for (byte aByte : bytes) {
			if (aByte == key) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean rem(byte key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] toByteArray() {
		return ByteList.of(bytes).toByteArray();
	}

	@Override
	public byte[] toArray(byte[] a) {
		return ByteList.of(bytes).toArray(a);
	}

	@Override
	public boolean addAll(ByteCollection c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsAll(ByteCollection c) {
		return ByteList.of(bytes).containsAll(c);
	}

	@Override
	public boolean removeAll(ByteCollection c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(ByteCollection c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void add(int index, byte key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(int index, ByteCollection c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte set(int index, byte k) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte getByte(int index) {
		return bytes[index];
	}

	@Override
	public int indexOf(byte k) {
		for (int i = 0; i < bytes.length; i++) {
			if (bytes[i] == k) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public int lastIndexOf(byte k) {
		for (int i = bytes.length - 1; i >= 0; i--) {
			if (bytes[i] == k) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public byte removeByte(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int compareTo(@NotNull List<? extends Byte> o) {
		var length1 = bytes.length;
		var length2 = o.size();
		if (length1 == length2) {
			int i = 0;
			for (byte ob : o) {
				if (bytes[i] != ob) {
					var compareResult = Byte.compareUnsigned(bytes[i], ob);
					if (compareResult != 0) {
						return compareResult;
					}
				}
				i++;
			}
			return 0;
		} else if (length1 > length2) {
			return 1;
		} else {
			return -1;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BinaryLexicographicList bytes1 = (BinaryLexicographicList) o;
		return Arrays.equals(bytes, bytes1.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}

	@Override
	public String toString() {
		return Arrays.toString(bytes);
	}
}
