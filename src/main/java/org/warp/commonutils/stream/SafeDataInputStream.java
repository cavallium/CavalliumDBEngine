/*
 * Copyright (c) 1994, 2019, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

package org.warp.commonutils.stream;

import java.io.DataInput;
import org.jetbrains.annotations.NotNull;

/**
 * A data input stream lets an application read primitive Java data
 * types from an underlying input stream in a machine-independent
 * way. An application uses a data output stream to write data that
 * can later be read by a data input stream.
 * <p>
 * DataInputStream is not necessarily safe for multithreaded access.
 * Thread safety is optional and is the responsibility of users of
 * methods in this class.
 *
 * @author  Arthur van Hoff
 * @see     java.io.DataOutputStream
 * @since   1.0
 */
public class SafeDataInputStream extends SafeFilterInputStream implements DataInput {

	/**
	 * Creates a DataInputStream that uses the specified
	 * underlying InputStream.
	 *
	 * @param  in   the specified input stream
	 */
	public SafeDataInputStream(SafeInputStream in) {
		super(in);
	}

	/**
	 * working arrays initialized on demand by readUTF
	 */
	private byte[] bytearr = new byte[80];
	private char[] chararr = new char[80];

	/**
	 * Reads some number of bytes from the contained input stream and
	 * stores them into the buffer array {@code b}. The number of
	 * bytes actually read is returned as an integer. This method blocks
	 * until input data is available, end of file is detected, or an
	 * exception is thrown.
	 *
	 * <p>If {@code b} is null, a {@code NullPointerException} is
	 * thrown. If the length of {@code b} is zero, then no bytes are
	 * read and {@code 0} is returned; otherwise, there is an attempt
	 * to read at least one byte. If no byte is available because the
	 * stream is at end of file, the value {@code -1} is returned;
	 * otherwise, at least one byte is read and stored into {@code b}.
	 *
	 * <p>The first byte read is stored into element {@code b[0]}, the
	 * next one into {@code b[1]}, and so on. The number of bytes read
	 * is, at most, equal to the length of {@code b}. Let {@code k}
	 * be the number of bytes actually read; these bytes will be stored in
	 * elements {@code b[0]} through {@code b[k-1]}, leaving
	 * elements {@code b[k]} through {@code b[b.length-1]}
	 * unaffected.
	 *
	 * <p>The {@code read(b)} method has the same effect as:
	 * <blockquote><pre>
	 * read(b, 0, b.length)
	 * </pre></blockquote>
	 *
	 * @param      b   the buffer into which the data is read.
	 * @return     the total number of bytes read into the buffer, or
	 *             {@code -1} if there is no more data because the end
	 *             of the stream has been reached.
	 * @see        SafeFilterInputStream#in
	 * @see        java.io.InputStream#read(byte[], int, int)
	 */
	public final int read(byte[] b) {
		return in.read(b, 0, b.length);
	}

	/**
	 * Reads up to {@code len} bytes of data from the contained
	 * input stream into an array of bytes.  An attempt is made to read
	 * as many as {@code len} bytes, but a smaller number may be read,
	 * possibly zero. The number of bytes actually read is returned as an
	 * integer.
	 *
	 * <p> This method blocks until input data is available, end of file is
	 * detected, or an exception is thrown.
	 *
	 * <p> If {@code len} is zero, then no bytes are read and
	 * {@code 0} is returned; otherwise, there is an attempt to read at
	 * least one byte. If no byte is available because the stream is at end of
	 * file, the value {@code -1} is returned; otherwise, at least one
	 * byte is read and stored into {@code b}.
	 *
	 * <p> The first byte read is stored into element {@code b[off]}, the
	 * next one into {@code b[off+1]}, and so on. The number of bytes read
	 * is, at most, equal to {@code len}. Let <i>k</i> be the number of
	 * bytes actually read; these bytes will be stored in elements
	 * {@code b[off]} through {@code b[off+}<i>k</i>{@code -1]},
	 * leaving elements {@code b[off+}<i>k</i>{@code ]} through
	 * {@code b[off+len-1]} unaffected.
	 *
	 * <p> In every case, elements {@code b[0]} through
	 * {@code b[off]} and elements {@code b[off+len]} through
	 * {@code b[b.length-1]} are unaffected.
	 *
	 * @param      b     the buffer into which the data is read.
	 * @param      off the start offset in the destination array {@code b}
	 * @param      len   the maximum number of bytes read.
	 * @return     the total number of bytes read into the buffer, or
	 *             {@code -1} if there is no more data because the end
	 *             of the stream has been reached.
	 * @throws     NullPointerException If {@code b} is {@code null}.
	 * @throws     IndexOutOfBoundsException If {@code off} is negative,
	 *             {@code len} is negative, or {@code len} is greater than
	 *             {@code b.length - off}
	 * @see        SafeFilterInputStream#in
	 * @see        java.io.InputStream#read(byte[], int, int)
	 */
	public final int read(byte[] b, int off, int len) {
		return in.read(b, off, len);
	}

	/**
	 * See the general contract of the {@code readFully}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @param   b   the buffer into which the data is read.
	 * @throws  NullPointerException if {@code b} is {@code null}.
	 * @see     SafeFilterInputStream#in
	 */
	public final void readFully(byte @NotNull [] b) {
		readFully(b, 0, b.length);
	}

	/**
	 * See the general contract of the {@code readFully}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @param      b     the buffer into which the data is read.
	 * @param      off   the start offset in the data array {@code b}.
	 * @param      len   the number of bytes to read.
	 * @throws     NullPointerException if {@code b} is {@code null}.
	 * @throws     IndexOutOfBoundsException if {@code off} is negative,
	 *             {@code len} is negative, or {@code len} is greater than
	 *             {@code b.length - off}.
	 * @see        SafeFilterInputStream#in
	 */
	public final void readFully(byte @NotNull [] b, int off, int len) {
		if (len < 0)
			throw new IndexOutOfBoundsException();
		int n = 0;
		while (n < len) {
			int count = in.read(b, off + n, len - n);
			if (count < 0)
				throw new IndexOutOfBoundsException();
			n += count;
		}
	}

	/**
	 * See the general contract of the {@code skipBytes}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes for this operation are read from the contained
	 * input stream.
	 *
	 * @param      n   the number of bytes to be skipped.
	 * @return     the actual number of bytes skipped.
	 */
	public final int skipBytes(int n) {
		int total = 0;
		int cur;

		while ((total<n) && ((cur = (int) in.skip(n-total)) > 0)) {
			total += cur;
		}

		return total;
	}

	/**
	 * See the general contract of the {@code readBoolean}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes for this operation are read from the contained
	 * input stream.
	 *
	 * @return     the {@code boolean} value read.
	 * @see        SafeFilterInputStream#in
	 */
	public final boolean readBoolean() {
		int ch = in.read();
		if (ch < 0)
			throw new IndexOutOfBoundsException();
		return (ch != 0);
	}

	/**
	 * See the general contract of the {@code readByte}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @return     the next byte of this input stream as a signed 8-bit
	 *             {@code byte}.
	 * @see        SafeFilterInputStream#in
	 */
	public final byte readByte() {
		int ch = in.read();
		if (ch < 0)
			throw new IndexOutOfBoundsException();
		return (byte)(ch);
	}

	/**
	 * See the general contract of the {@code readUnsignedByte}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @return     the next byte of this input stream, interpreted as an
	 *             unsigned 8-bit number.
	 * @see         SafeFilterInputStream#in
	 */
	public final int readUnsignedByte() {
		int ch = in.read();
		if (ch < 0)
			throw new IndexOutOfBoundsException();
		return ch;
	}

	/**
	 * See the general contract of the {@code readShort}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @return     the next two bytes of this input stream, interpreted as a
	 *             signed 16-bit number.
	 * @see        SafeFilterInputStream#in
	 */
	public final short readShort() {
		int ch1 = in.read();
		int ch2 = in.read();
		if ((ch1 | ch2) < 0)
			throw new IndexOutOfBoundsException();
		return (short)((ch1 << 8) + (ch2 << 0));
	}

	/**
	 * See the general contract of the {@code readUnsignedShort}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @return     the next two bytes of this input stream, interpreted as an
	 *             unsigned 16-bit integer.
	 * @see        SafeFilterInputStream#in
	 */
	public final int readUnsignedShort() {
		int ch1 = in.read();
		int ch2 = in.read();
		if ((ch1 | ch2) < 0)
			throw new IndexOutOfBoundsException();
		return (ch1 << 8) + (ch2 << 0);
	}

	/**
	 * See the general contract of the {@code readChar}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @return     the next two bytes of this input stream, interpreted as a
	 *             {@code char}.
	 * @see        SafeFilterInputStream#in
	 */
	public final char readChar() {
		int ch1 = in.read();
		int ch2 = in.read();
		if ((ch1 | ch2) < 0)
			throw new IndexOutOfBoundsException();
		return (char)((ch1 << 8) + (ch2 << 0));
	}

	/**
	 * See the general contract of the {@code readInt}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @return     the next four bytes of this input stream, interpreted as an
	 *             {@code int}.
	 * @see        SafeFilterInputStream#in
	 */
	public final int readInt() {
		int ch1 = in.read();
		int ch2 = in.read();
		int ch3 = in.read();
		int ch4 = in.read();
		if ((ch1 | ch2 | ch3 | ch4) < 0)
			throw new IndexOutOfBoundsException();
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	private final byte[] readBuffer = new byte[8];

	/**
	 * See the general contract of the {@code readLong}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @return     the next eight bytes of this input stream, interpreted as a
	 *             {@code long}.
	 * @see        SafeFilterInputStream#in
	 */
	public final long readLong() {
		readFully(readBuffer, 0, 8);
		return (((long)readBuffer[0] << 56) +
				((long)(readBuffer[1] & 255) << 48) +
				((long)(readBuffer[2] & 255) << 40) +
				((long)(readBuffer[3] & 255) << 32) +
				((long)(readBuffer[4] & 255) << 24) +
				((readBuffer[5] & 255) << 16) +
				((readBuffer[6] & 255) <<  8) +
				((readBuffer[7] & 255) <<  0));
	}

	/**
	 * See the general contract of the {@code readFloat}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @return     the next four bytes of this input stream, interpreted as a
	 *             {@code float}.
	 * @see        SafeDataInputStream#readInt()
	 * @see        Float#intBitsToFloat(int)
	 */
	public final float readFloat() {
		return Float.intBitsToFloat(readInt());
	}

	/**
	 * See the general contract of the {@code readDouble}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @return     the next eight bytes of this input stream, interpreted as a
	 *             {@code double}.
	 * @see        SafeDataInputStream#readLong()
	 * @see        Double#longBitsToDouble(long)
	 */
	public final double readDouble() {
		return Double.longBitsToDouble(readLong());
	}

	private char[] lineBuffer;

	/**
	 * See the general contract of the {@code readLine}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @deprecated This method does not properly convert bytes to characters.
	 * As of JDK&nbsp;1.1, the preferred way to read lines of text is via the
	 * {@code BufferedReader.readLine()} method.  Programs that use the
	 * {@code DataInputStream} class to read lines can be converted to use
	 * the {@code BufferedReader} class by replacing code of the form:
	 * <blockquote><pre>
	 *     DataInputStream d =&nbsp;new&nbsp;DataInputStream(in);
	 * </pre></blockquote>
	 * with:
	 * <blockquote><pre>
	 *     BufferedReader d
	 *          =&nbsp;new&nbsp;BufferedReader(new&nbsp;InputStreamReader(in));
	 * </pre></blockquote>
	 *
	 * @return     the next line of text from this input stream.
	 * @see        java.io.BufferedReader#readLine()
	 * @see        SafeFilterInputStream#in
	 */
	@Deprecated
	public final String readLine() {
		char[] buf = lineBuffer;

		if (buf == null) {
			buf = lineBuffer = new char[128];
		}

		int room = buf.length;
		int offset = 0;
		int c;

		loop:   while (true) {
			switch (c = in.read()) {
				case -1:
				case '\n':
					break loop;

				case '\r':
					int c2 = in.read();
					if ((c2 != '\n') && (c2 != -1)) {
						if (!(in instanceof SafePushbackInputStream)) {
							this.in = new SafePushbackInputStream(in);
						}
						((SafePushbackInputStream)in).unread(c2);
					}
					break loop;

				default:
					if (--room < 0) {
						buf = new char[offset + 128];
						room = buf.length - offset - 1;
						System.arraycopy(lineBuffer, 0, buf, 0, offset);
						lineBuffer = buf;
					}
					buf[offset++] = (char) c;
					break;
			}
		}
		if ((c == -1) && (offset == 0)) {
			return null;
		}
		return String.copyValueOf(buf, 0, offset);
	}

	/**
	 * See the general contract of the {@code readUTF}
	 * method of {@code DataInput}.
	 * <p>
	 * Bytes
	 * for this operation are read from the contained
	 * input stream.
	 *
	 * @return     a Unicode string.
	 * @see        SafeDataInputStream#readUTF(SafeDataInputStream)
	 */
	public final @NotNull String readUTF() {
		return readUTF(this);
	}

	/**
	 * Reads from the
	 * stream {@code in} a representation
	 * of a Unicode  character string encoded in
	 * <a href="DataInput.html#modified-utf-8">modified UTF-8</a> format;
	 * this string of characters is then returned as a {@code String}.
	 * The details of the modified UTF-8 representation
	 * are  exactly the same as for the {@code readUTF}
	 * method of {@code DataInput}.
	 *
	 * @param      in   a data input stream.
	 * @return     a Unicode string.
	 * @see        SafeDataInputStream#readUnsignedShort()
	 */
	public static String readUTF(SafeDataInputStream in) {
		int utflen = in.readUnsignedShort();
		byte[] bytearr;
		char[] chararr;
		if (in.bytearr.length < utflen){
			in.bytearr = new byte[utflen*2];
			in.chararr = new char[utflen*2];
		}
		chararr = in.chararr;
		bytearr = in.bytearr;

		int c, char2, char3;
		int count = 0;
		int chararr_count=0;

		in.readFully(bytearr, 0, utflen);

		while (count < utflen) {
			c = (int) bytearr[count] & 0xff;
			if (c > 127) break;
			count++;
			chararr[chararr_count++]=(char)c;
		}

		while (count < utflen) {
			c = (int) bytearr[count] & 0xff;
			switch (c >> 4) {
				case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
					/* 0xxxxxxx*/
					count++;
					chararr[chararr_count++]=(char)c;
					break;
				case 12: case 13:
					/* 110x xxxx   10xx xxxx*/
					count += 2;
					if (count > utflen)
						throw new IllegalArgumentException(
								"malformed input: partial character at end");
					char2 = bytearr[count-1];
					if ((char2 & 0xC0) != 0x80)
						throw new IllegalArgumentException(
								"malformed input around byte " + count);
					chararr[chararr_count++]=(char)(((c & 0x1F) << 6) |
							(char2 & 0x3F));
					break;
				case 14:
					/* 1110 xxxx  10xx xxxx  10xx xxxx */
					count += 3;
					if (count > utflen)
						throw new IllegalArgumentException(
								"malformed input: partial character at end");
					char2 = bytearr[count-2];
					char3 = bytearr[count-1];
					if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
						throw new IllegalArgumentException(
								"malformed input around byte " + (count-1));
					chararr[chararr_count++]=(char)(((c     & 0x0F) << 12) |
							((char2 & 0x3F) << 6)  |
							((char3 & 0x3F) << 0));
					break;
				default:
					/* 10xx xxxx,  1111 xxxx */
					throw new IllegalArgumentException(
							"malformed input around byte " + count);
			}
		}
		// The number of chars produced may be less than utflen
		return new String(chararr, 0, chararr_count);
	}
}
