/*
 * Copyright (C) 2005-2022 Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.warp.commonutils.stream;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.bytes.ByteArrays;

/** Simple, fast byte-array output stream that exposes the backing array.
 *
 * <p>{@link java.io.ByteArrayOutputStream} is nice, but to get its content you
 * must generate each time a new object. This doesn't happen here.
 *
 * <p>This class will automatically enlarge the backing array, doubling its
 * size whenever new space is needed. The {@link #reset()} method will
 * mark the content as empty, but will not decrease the capacity: use
 * {@link #trim()} for that purpose.
 *
 * @author Sebastiano Vigna
 */

public class SafeByteArrayOutputStream extends SafeMeasurableOutputStream implements SafeRepositionableStream {

	/** The array backing the output stream. */
	public static final int DEFAULT_INITIAL_CAPACITY = 16;
	private final boolean wrapped;
	private final int initialPosition;
	private final int initialLength;

	/** The array backing the output stream. */
	public byte[] array;

	/** The number of valid bytes in {@link #array}. */
	public int length;

	/** The current writing position. */
	private int position;

	/** Creates a new array output stream with an initial capacity of {@link #DEFAULT_INITIAL_CAPACITY} bytes. */
	public SafeByteArrayOutputStream() {
		this(DEFAULT_INITIAL_CAPACITY);
	}

	/** Creates a new array output stream with a given initial capacity.
	 *
	 * @param initialCapacity the initial length of the backing array.
	 */
	public SafeByteArrayOutputStream(final int initialCapacity) {
		array = new byte[initialCapacity];
		wrapped = false;
		initialPosition = 0;
		initialLength = 0;
	}

	/** Creates a new array output stream wrapping a given byte array.
	 *
	 * @param a the byte array to wrap.
	 */
	public SafeByteArrayOutputStream(final byte[] a) {
		array = a;
		wrapped = true;
		initialPosition = 0;
		initialLength = a.length;
	}

	/** Creates a new array output stream wrapping a given byte array.
	 *
	 * @param a the byte array to wrap.
	 */
	public SafeByteArrayOutputStream(final byte[] a, int from, int to) {
		Arrays.ensureFromTo(a.length, from, to);
		wrapped = true;
		array = a;
		initialPosition = from;
		initialLength = to;
		position = from;
		length = to - from;
	}

	/** Marks this array output stream as empty. */
	public void reset() {
		length = initialLength;
		position = initialPosition;
	}

	/** Ensures that the length of the backing array is equal to {@link #length}. */
	public void trim() {
		if (!wrapped) {
			array = ByteArrays.trim(array, length);
		}
	}

	public void ensureWritable(int size) {
		growBy(size);
	}

	@Override
	public void write(final int b) {
		if (position >= array.length) {
			if (wrapped) {
				throw new ArrayIndexOutOfBoundsException(position);
			} else {
				array = ByteArrays.grow(array, position + 1, length);
			}
		}
		array[position++] = (byte)b;
		if (length < position) length = position;
	}

	@Override
	public void write(final byte[] b, final int off, final int len) {
		ByteArrays.ensureOffsetLength(b, off, len);
		growBy(len);
		System.arraycopy(b, off, array, position, len);
		if (position + len > length) length = position += len;
	}

	private void growBy(int len) {
		if (position + len > array.length) {
			if (wrapped) {
				throw new ArrayIndexOutOfBoundsException(position + len - 1);
			} else {
				array = ByteArrays.grow(array, position + len, position);
			}
		}
	}

	@Override
	public void position(final long newPosition) {
		position = (int)newPosition;
	}

	@Override
	public long position() {
		return position;
	}

	@Override
	public long length() {
		return length;
	}
}
