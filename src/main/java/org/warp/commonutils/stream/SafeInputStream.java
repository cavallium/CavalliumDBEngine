package org.warp.commonutils.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class SafeInputStream extends InputStream {

	// MAX_SKIP_BUFFER_SIZE is used to determine the maximum buffer size to
	// use when skipping.
	private static final int MAX_SKIP_BUFFER_SIZE = 2048;

	private static final int DEFAULT_BUFFER_SIZE = 8192;

	@Override
	public abstract int read();

	public int read(byte b[]) {
		return read(b, 0, b.length);
	}

	public int read(byte b[], int off, int len) {
		Objects.checkFromIndexSize(off, len, b.length);
		if (len == 0) {
			return 0;
		}

		int c = read();
		if (c == -1) {
			return -1;
		}
		b[off] = (byte)c;

		int i = 1;
		for (; i < len ; i++) {
			c = read();
			if (c == -1) {
				break;
			}
			b[off + i] = (byte)c;
		}
		return i;
	}

	public byte[] readAllBytes() {
		return readNBytes(Integer.MAX_VALUE);
	}

	private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

	public byte[] readNBytes(int len) {
		if (len < 0) {
			throw new IllegalArgumentException("len < 0");
		}

		List<byte[]> bufs = null;
		byte[] result = null;
		int total = 0;
		int remaining = len;
		int n;
		do {
			byte[] buf = new byte[Math.min(remaining, DEFAULT_BUFFER_SIZE)];
			int nread = 0;

			// read to EOF which may read more or less than buffer size
			while ((n = read(buf, nread,
					Math.min(buf.length - nread, remaining))) > 0) {
				nread += n;
				remaining -= n;
			}

			if (nread > 0) {
				if (MAX_BUFFER_SIZE - total < nread) {
					throw new OutOfMemoryError("Required array size too large");
				}
				total += nread;
				if (result == null) {
					result = buf;
				} else {
					if (bufs == null) {
						bufs = new ArrayList<>();
						bufs.add(result);
					}
					bufs.add(buf);
				}
			}
			// if the last call to read returned -1 or the number of bytes
			// requested have been read then break
		} while (n >= 0 && remaining > 0);

		if (bufs == null) {
			if (result == null) {
				return new byte[0];
			}
			return result.length == total ?
					result : Arrays.copyOf(result, total);
		}

		result = new byte[total];
		int offset = 0;
		remaining = total;
		for (byte[] b : bufs) {
			int count = Math.min(b.length, remaining);
			System.arraycopy(b, 0, result, offset, count);
			offset += count;
			remaining -= count;
		}

		return result;
	}

	public int readNBytes(byte[] b, int off, int len) {
		Objects.checkFromIndexSize(off, len, b.length);

		int n = 0;
		while (n < len) {
			int count = read(b, off + n, len - n);
			if (count < 0)
				break;
			n += count;
		}
		return n;
	}

	public long skip(long n) {
		long remaining = n;
		int nr;

		if (n <= 0) {
			return 0;
		}

		int size = (int)Math.min(MAX_SKIP_BUFFER_SIZE, remaining);
		byte[] skipBuffer = new byte[size];
		while (remaining > 0) {
			nr = read(skipBuffer, 0, (int)Math.min(size, remaining));
			if (nr < 0) {
				break;
			}
			remaining -= nr;
		}

		return n - remaining;
	}

	public void skipNBytes(long n) {
		if (n > 0) {
			long ns = skip(n);
			if (ns >= 0 && ns < n) { // skipped too few bytes
				// adjust number to skip
				n -= ns;
				// read until requested number skipped or EOS reached
				while (n > 0 && read() != -1) {
					n--;
				}
				// if not enough skipped, then EOFE
				if (n != 0) {
					throw new IndexOutOfBoundsException();
				}
			} else if (ns != n) { // skipped negative or too many bytes
				throw new IllegalArgumentException("Unable to skip exactly");
			}
		}
	}

	public int available() {
		return 0;
	}
	
	public void close() {}

	public void mark(int readlimit) {}

	public void reset() {
		throw new UnsupportedOperationException("mark/reset not supported");
	}
	
	public boolean markSupported() {
		return false;
	}

	public long transferTo(OutputStream out) {
		Objects.requireNonNull(out, "out");
		long transferred = 0;
		byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
		int read;
		while ((read = this.read(buffer, 0, DEFAULT_BUFFER_SIZE)) >= 0) {
			try {
				out.write(buffer, 0, read);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
			transferred += read;
		}
		return transferred;
	}
}
