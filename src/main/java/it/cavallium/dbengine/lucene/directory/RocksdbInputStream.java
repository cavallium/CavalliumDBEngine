package it.cavallium.dbengine.lucene.directory;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;
import java.io.EOFException;
import java.io.IOException;
import org.apache.lucene.store.IndexInput;

public class RocksdbInputStream extends IndexInput {

	private final int bufferSize;

	private long position;

	private final long length;

	private Buffer currentBuffer;

	private int currentBufferIndex;

	private boolean closed = false;

	private final RocksdbFileStore store;

	private final String name;

	public RocksdbInputStream(String name, RocksdbFileStore store, int bufferSize) throws IOException {
		this(name, store, bufferSize, store.getSize(name));
	}

	public RocksdbInputStream(String name, RocksdbFileStore store, int bufferSize, long length) {
		this(name,
				store,
				bufferSize,
				length,
				null
		);
	}

	private RocksdbInputStream(String name, RocksdbFileStore store, int bufferSize, long length, Buffer currentBuffer) {
		super("RocksdbInputStream(name=" + name + ")");
		this.name = name;
		this.store = store;
		this.bufferSize = bufferSize;
		this.currentBuffer = currentBuffer;
		this.currentBufferIndex = bufferSize;
		this.position = 0;
		this.length = length;
		if (currentBuffer != null && bufferSize > currentBuffer.capacity()) {
			throw new IllegalArgumentException(
					"BufferSize is " + bufferSize + " but the buffer has only a capacity of " + currentBuffer.capacity());
		}
	}

	@Override
	public void close() throws IOException {
		if (!closed) {
			closed = true;
			if (currentBuffer != null) {
				currentBuffer.close();
			}
		}
	}

	@Override
	public long getFilePointer() {
		return position;
	}

	@Override
	public void seek(long pos) {
		if (pos < 0 || pos > length) {
			throw new IllegalArgumentException("pos must be between 0 and " + length);
		}
		position = pos;
		currentBufferIndex = this.bufferSize;
	}

	@Override
	public long length() {
		return this.length;
	}

	@Override
	public IndexInput slice(String sliceDescription, final long offset, final long length) throws IOException {

		if (offset < 0 || length < 0 || offset + length > this.length) {
			throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this);
		}

		return new RocksDBSliceInputStream(name,
				store,
				bufferSize,
				offset + length
		) {
			{
				seek(0L);
			}

			@Override
			public void seek(long pos) {
				if (pos < 0L) {
					throw new IllegalArgumentException("Seeking to negative position: " + this);
				}

				super.seek(pos + offset);
			}


			@Override
			public long getFilePointer() {
				return super.getFilePointer() - offset;
			}

			@Override
			public long length() {
				return super.length() - offset;
			}

			@Override
			public IndexInput slice(String sliceDescription, long ofs, long len) throws IOException {
				return super.slice(sliceDescription, offset + ofs, len);
			}
		};
	}


	@Override
	public byte readByte() throws IOException {

		if (position >= length) {
			throw new EOFException("Read end");
		}
		loadBufferIfNeed();
		byte b = currentBuffer.getByte(currentBufferIndex++);
		position++;
		return b;
	}

	protected void loadBufferIfNeed() throws IOException {
		if (currentBuffer == null) {
			currentBuffer = store.bufferAllocator.allocate(bufferSize).writerOffset(bufferSize);
		}
		if (this.currentBufferIndex == this.bufferSize) {
			int n = store.load(name, position, currentBuffer, 0, bufferSize);
			if (n == -1) {
				throw new EOFException("Read end");
			}
			this.currentBufferIndex = 0;
		}
	}

	@Override
	public void readBytes(byte[] b, int offset, int len) throws IOException {

		if (position >= length) {
			throw new EOFException("Read end");
		}

		int f = offset;
		int n = Math.min((int) (length - position), len);
		do {
			loadBufferIfNeed();

			int r = Math.min(bufferSize - currentBufferIndex, n);

			currentBuffer.copyInto(currentBufferIndex, b, f, r);

			f += r;
			position += r;
			currentBufferIndex += r;
			n -= r;

		} while (n != 0);
	}

	@Override
	public IndexInput clone() {
		return super.clone();
	}
}