package org.warp.commonutils.stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;

/**
 * Simple {@link InputStream} implementation that exposes currently
 * available content of a {@link ByteBuffer}.
 */
public class ByteBufferBackedInputStream extends InputStream {
	protected final ByteBuffer _b;

	public ByteBufferBackedInputStream(ByteBuffer buf) { _b = buf; }

	@Override public int available() { return _b.remaining(); }

	@Override
	public int read() { return _b.hasRemaining() ? (_b.get() & 0xFF) : -1; }

	@Override
	public int read(byte @NotNull [] bytes, int off, int len) {
		if (!_b.hasRemaining()) return -1;
		len = Math.min(len, _b.remaining());
		_b.get(bytes, off, len);
		return len;
	}
}