package org.warp.commonutils.stream;

/**
 * A {@code FilterInputStream} contains
 * some other input stream, which it uses as
 * its  basic source of data, possibly transforming
 * the data along the way or providing  additional
 * functionality. The class {@code FilterInputStream}
 * itself simply overrides all  methods of
 * {@code InputStream} with versions that
 * pass all requests to the contained  input
 * stream. Subclasses of {@code FilterInputStream}
 * may further override some of  these methods
 * and may also provide additional methods
 * and fields.
 *
 * @author  Jonathan Payne
 * @since   1.0
 */
public class SafeFilterInputStream extends SafeInputStream {
	/**
	 * The input stream to be filtered.
	 */
	protected volatile SafeInputStream in;

	/**
	 * Creates a {@code FilterInputStream}
	 * by assigning the  argument {@code in}
	 * to the field {@code this.in} so as
	 * to remember it for later use.
	 *
	 * @param   in   the underlying input stream, or {@code null} if
	 *          this instance is to be created without an underlying stream.
	 */
	protected SafeFilterInputStream(SafeInputStream in) {
		this.in = in;
	}

	/**
	 * Reads the next byte of data from this input stream. The value
	 * byte is returned as an {@code int} in the range
	 * {@code 0} to {@code 255}. If no byte is available
	 * because the end of the stream has been reached, the value
	 * {@code -1} is returned. This method blocks until input data
	 * is available, the end of the stream is detected, or an exception
	 * is thrown.
	 * <p>
	 * This method
	 * simply performs {@code in.read()} and returns the result.
	 *
	 * @return     the next byte of data, or {@code -1} if the end of the
	 *             stream is reached.
	 * @see        SafeFilterInputStream#in
	 */
	public int read() {
		return in.read();
	}

	/**
	 * Reads up to {@code b.length} bytes of data from this
	 * input stream into an array of bytes. This method blocks until some
	 * input is available.
	 * <p>
	 * This method simply performs the call
	 * {@code read(b, 0, b.length)} and returns
	 * the  result. It is important that it does
	 * <i>not</i> do {@code in.read(b)} instead;
	 * certain subclasses of  {@code FilterInputStream}
	 * depend on the implementation strategy actually
	 * used.
	 *
	 * @param      b   the buffer into which the data is read.
	 * @return     the total number of bytes read into the buffer, or
	 *             {@code -1} if there is no more data because the end of
	 *             the stream has been reached.
	 * @see        SafeFilterInputStream#read(byte[], int, int)
	 */
	public int read(byte b[]) {
		return read(b, 0, b.length);
	}

	/**
	 * Reads up to {@code len} bytes of data from this input stream
	 * into an array of bytes. If {@code len} is not zero, the method
	 * blocks until some input is available; otherwise, no
	 * bytes are read and {@code 0} is returned.
	 * <p>
	 * This method simply performs {@code in.read(b, off, len)}
	 * and returns the result.
	 *
	 * @param      b     the buffer into which the data is read.
	 * @param      off   the start offset in the destination array {@code b}
	 * @param      len   the maximum number of bytes read.
	 * @return     the total number of bytes read into the buffer, or
	 *             {@code -1} if there is no more data because the end of
	 *             the stream has been reached.
	 * @throws     NullPointerException If {@code b} is {@code null}.
	 * @throws     IndexOutOfBoundsException If {@code off} is negative,
	 *             {@code len} is negative, or {@code len} is greater than
	 *             {@code b.length - off}
	 * @see        SafeFilterInputStream#in
	 */
	public int read(byte b[], int off, int len) {
		return in.read(b, off, len);
	}

	/**
	 * Skips over and discards {@code n} bytes of data from the
	 * input stream. The {@code skip} method may, for a variety of
	 * reasons, end up skipping over some smaller number of bytes,
	 * possibly {@code 0}. The actual number of bytes skipped is
	 * returned.
	 * <p>
	 * This method simply performs {@code in.skip(n)}.
	 *
	 * @param      n   the number of bytes to be skipped.
	 * @return     the actual number of bytes skipped.
	 */
	public long skip(long n) {
		return in.skip(n);
	}

	/**
	 * Returns an estimate of the number of bytes that can be read (or
	 * skipped over) from this input stream without blocking by the next
	 * caller of a method for this input stream. The next caller might be
	 * the same thread or another thread.  A single read or skip of this
	 * many bytes will not block, but may read or skip fewer bytes.
	 * <p>
	 * This method returns the result of {@link #in in}.available().
	 *
	 * @return     an estimate of the number of bytes that can be read (or skipped
	 *             over) from this input stream without blocking.
	 */
	public int available() {
		return in.available();
	}

	/**
	 * Closes this input stream and releases any system resources
	 * associated with the stream.
	 * This
	 * method simply performs {@code in.close()}.
	 *
	 * @see        SafeFilterInputStream#in
	 */
	public void close() {
		in.close();
	}

	/**
	 * Marks the current position in this input stream. A subsequent
	 * call to the {@code reset} method repositions this stream at
	 * the last marked position so that subsequent reads re-read the same bytes.
	 * <p>
	 * The {@code readlimit} argument tells this input stream to
	 * allow that many bytes to be read before the mark position gets
	 * invalidated.
	 * <p>
	 * This method simply performs {@code in.mark(readlimit)}.
	 *
	 * @param   readlimit   the maximum limit of bytes that can be read before
	 *                      the mark position becomes invalid.
	 * @see     SafeFilterInputStream#in
	 * @see     SafeFilterInputStream#reset()
	 */
	public void mark(int readlimit) {
		in.mark(readlimit);
	}

	/**
	 * Repositions this stream to the position at the time the
	 * {@code mark} method was last called on this input stream.
	 * <p>
	 * This method
	 * simply performs {@code in.reset()}.
	 * <p>
	 * Stream marks are intended to be used in
	 * situations where you need to read ahead a little to see what's in
	 * the stream. Often this is most easily done by invoking some
	 * general parser. If the stream is of the type handled by the
	 * parse, it just chugs along happily. If the stream is not of
	 * that type, the parser should toss an exception when it fails.
	 * If this happens within readlimit bytes, it allows the outer
	 * code to reset the stream and try another parser.
	 *
	 * @see        SafeFilterInputStream#in
	 * @see        SafeFilterInputStream#mark(int)
	 */
	public void reset() {
		in.reset();
	}

	/**
	 * Tests if this input stream supports the {@code mark}
	 * and {@code reset} methods.
	 * This method
	 * simply performs {@code in.markSupported()}.
	 *
	 * @return  {@code true} if this stream type supports the
	 *          {@code mark} and {@code reset} method;
	 *          {@code false} otherwise.
	 * @see     SafeFilterInputStream#in
	 * @see     java.io.InputStream#mark(int)
	 * @see     java.io.InputStream#reset()
	 */
	public boolean markSupported() {
		return in.markSupported();
	}
}
