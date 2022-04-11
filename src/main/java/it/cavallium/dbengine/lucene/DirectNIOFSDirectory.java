package it.cavallium.dbengine.lucene;

import static it.cavallium.dbengine.lucene.LuceneUtils.alignUnsigned;
import static it.cavallium.dbengine.lucene.LuceneUtils.readInternalAligned;

import com.sun.nio.file.ExtendedOpenOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.IOUtils;

@SuppressWarnings({"RedundantArrayCreation", "unused", "unused", "RedundantCast"})
public class DirectNIOFSDirectory extends FSDirectory {

	@SuppressWarnings("sunapi")
	private final OpenOption[] openOptions = {StandardOpenOption.READ, com.sun.nio.file.ExtendedOpenOption.DIRECT};

	public DirectNIOFSDirectory(Path path, LockFactory lockFactory) throws IOException {
		super(path, lockFactory);
	}

	public DirectNIOFSDirectory(Path path) throws IOException {
		this(path, FSLockFactory.getDefault());
	}

	@Override
	public IndexInput openInput(String name, IOContext context) throws IOException {
		this.ensureOpen();
		this.ensureCanRead(name);
		Path path = this.getDirectory().resolve(name);
		FileChannel fc = FileChannel.open(path, openOptions);
		boolean success = false;

		DirectNIOFSDirectory.NIOFSIndexInput var7;
		try {
			DirectNIOFSDirectory.NIOFSIndexInput indexInput = new DirectNIOFSDirectory.NIOFSIndexInput("NIOFSIndexInput(path=\"" + path + "\")", fc, context);
			success = true;
			var7 = indexInput;
		} finally {
			if (!success) {
				IOUtils.closeWhileHandlingException(new Closeable[]{fc});
			}

		}

		return var7;
	}

	static final class NIOFSIndexInput extends BufferedIndexInput {
		private static final int CHUNK_SIZE = 16384;
		private final FileChannel channel;
		boolean isClone = false;
		private final long off;
		private final long end;

		public NIOFSIndexInput(String resourceDesc, FileChannel fc, IOContext context) throws IOException {
			super(resourceDesc, context);
			this.channel = fc;
			this.off = 0L;
			this.end = fc.size();
		}

		public NIOFSIndexInput(String resourceDesc, FileChannel fc, long off, long length, int bufferSize) {
			super(resourceDesc, bufferSize);
			this.channel = fc;
			this.off = off;
			this.end = off + length;
			this.isClone = true;
		}

		public void close() throws IOException {
			if (!this.isClone) {
				this.channel.close();
			}

		}

		public DirectNIOFSDirectory.NIOFSIndexInput clone() {
			DirectNIOFSDirectory.NIOFSIndexInput clone = (DirectNIOFSDirectory.NIOFSIndexInput)super.clone();
			clone.isClone = true;
			return clone;
		}

		public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
			if (offset >= 0L && length >= 0L && offset + length <= this.length()) {
				return new DirectNIOFSDirectory.NIOFSIndexInput(this.getFullSliceDescription(sliceDescription), this.channel, this.off + offset, length, this.getBufferSize());
			} else {
				throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset + ",length=" + length + ",fileLength=" + this.length() + ": " + this);
			}
		}

		public long length() {
			return this.end - this.off;
		}

		protected void readInternal(ByteBuffer b) throws IOException {
			long pos = this.getFilePointer() + this.off;
			if (pos + (long)b.remaining() > this.end) {
				throw new EOFException("read past EOF: " + this);
			}

			try {
				if (pos % 4096 == 0 && b.remaining() % 4096 == 0) {
					readInternalAligned(this, this.channel, pos, b, b.remaining(), b.remaining(), end);
				} else {
					long startOffsetAligned = alignUnsigned(pos, false);
					int size = b.remaining();
					long endOffsetAligned = alignUnsigned(pos + size, true);
					long expectedTempBufferSize = endOffsetAligned - startOffsetAligned;
					if (expectedTempBufferSize > Integer.MAX_VALUE || expectedTempBufferSize < 0) {
						throw new IllegalStateException("Invalid temp buffer size: " + expectedTempBufferSize);
					}
					ByteBuffer alignedBuf = ByteBuffer.allocate((int) expectedTempBufferSize);
					int sliceStartOffset = (int) (pos - startOffsetAligned);
					int sliceEndOffset = sliceStartOffset + (int) size;
					readInternalAligned(this, this.channel, startOffsetAligned, alignedBuf, (int) expectedTempBufferSize, sliceEndOffset, end);
					var slice = alignedBuf.slice(sliceStartOffset, sliceEndOffset - sliceStartOffset);
					b.put(slice.array(), slice.arrayOffset(), sliceEndOffset - sliceStartOffset);
					b.limit(b.position());
				}
			} catch (IOException var7) {
				throw new IOException(var7.getMessage() + ": " + this, var7);
			}
		}

		protected void seekInternal(long pos) throws IOException {
			if (pos > this.length()) {
				throw new EOFException("read past EOF: pos=" + pos + " vs length=" + this.length() + ": " + this);
			}
		}
	}
}
