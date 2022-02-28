package it.cavallium.dbengine.lucene.directory;

import io.net5.buffer.ByteBuf;
import io.net5.buffer.ByteBufAllocator;
import io.net5.buffer.Unpooled;
import java.io.EOFException;
import java.io.IOException;
import org.apache.lucene.store.IndexInput;

public class RocksdbInputStream extends IndexInput {

	private final int bufferSize;

	private long position;

	private final long length;

	private ByteBuf currentBuffer;

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
				ByteBufAllocator.DEFAULT.ioBuffer(bufferSize, bufferSize).writerIndex(bufferSize)
		);
	}

	private RocksdbInputStream(String name, RocksdbFileStore store, int bufferSize, long length, ByteBuf currentBuffer) {
		super("RocksdbInputStream(name=" + name + ")");
		this.name = name;
		this.store = store;
		this.bufferSize = bufferSize;
		this.currentBuffer = currentBuffer;
		currentBuffer.readerIndex(bufferSize);
		this.position = 0;
		this.length = length;
	}

	@Override
	public void close() throws IOException {
		currentBuffer.release();
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
		currentBuffer.readerIndex(bufferSize);
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

		return new RocksdbInputStream(name,
				store,
				bufferSize,
				offset + length,
				Unpooled.buffer(bufferSize, bufferSize).writerIndex(bufferSize)
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
		byte b = currentBuffer.readByte();
		position++;
		return b;
	}

	protected void loadBufferIfNeed() throws IOException {
		if (currentBuffer.readerIndex() == this.bufferSize) {
			int n = store.load(name, position, currentBuffer, 0, bufferSize);
			if (n == -1) {
				throw new EOFException("Read end");
			}
			currentBuffer.readerIndex(0);
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

			int r = Math.min(bufferSize - currentBuffer.readerIndex(), n);

			currentBuffer.readBytes(b, f, r);

			f += r;
			position += r;
			n -= r;

		} while (n != 0);
	}

	@Override
	public IndexInput clone() {
		RocksdbInputStream in = (RocksdbInputStream) super.clone();
		in.currentBuffer = in.currentBuffer.duplicate();
		return in;
	}
}