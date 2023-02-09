package it.cavallium.dbengine.lucene;

import static it.cavallium.dbengine.lucene.LuceneUtils.warnLuceneThread;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

public class CheckIndexInput extends IndexInput {

	private final IndexInput input;

	public CheckIndexInput(IndexInput input) {
		super(input.toString());
		this.input = input;
	}

	private static void checkThread() {
		assert LuceneUtils.isLuceneThread();
	}

	@Override
	public void close() throws IOException {
		warnLuceneThread();
		input.close();
	}

	@Override
	public long getFilePointer() {
		checkThread();
		return input.getFilePointer();
	}

	@Override
	public void seek(long pos) throws IOException {
		checkThread();
		input.seek(pos);
	}

	@Override
	public long length() {
		checkThread();
		return input.length();
	}

	@Override
	public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
		checkThread();
		return input.slice(sliceDescription, offset, length);
	}

	@Override
	public byte readByte() throws IOException {
		checkThread();
		return input.readByte();
	}

	@Override
	public void readBytes(byte[] b, int offset, int len) throws IOException {
		checkThread();
		input.readBytes(b, offset, len);
	}

	@Override
	public void skipBytes(long numBytes) throws IOException {
		checkThread();
		input.skipBytes(numBytes);
	}

	@Override
	public IndexInput clone() {
		return new CheckIndexInput(input.clone());
	}

	@Override
	public String toString() {
		checkThread();
		return input.toString();
	}

	@Override
	public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
		var ras = input.randomAccessSlice(offset, length);
		return new RandomAccessInput() {
			@Override
			public byte readByte(long pos) throws IOException {
				checkThread();
				return ras.readByte(pos);
			}

			@Override
			public short readShort(long pos) throws IOException {
				checkThread();
				return ras.readShort(pos);
			}

			@Override
			public int readInt(long pos) throws IOException {
				checkThread();
				return ras.readInt(pos);
			}

			@Override
			public long readLong(long pos) throws IOException {
				checkThread();
				return ras.readLong(pos);
			}
		};
	}
}
