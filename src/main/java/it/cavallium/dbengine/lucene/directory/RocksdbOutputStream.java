package it.cavallium.dbengine.lucene.directory;

import io.net5.buffer.ByteBuf;
import io.net5.buffer.ByteBufAllocator;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;

import java.io.IOException;
import java.util.Collection;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class RocksdbOutputStream extends IndexOutput implements Accountable {

	private final int bufferSize;

	private long position;

	private ByteBuf currentBuffer;

	private boolean dirty;

	private final Checksum crc;

	private final RocksdbFileStore store;

	private final String name;

	public RocksdbOutputStream(String name, RocksdbFileStore store, int bufferSize, boolean checksum) {
		super("RocksdbOutputStream(name=" + name + ")", name);
		this.name = name;
		this.store = store;
		this.bufferSize = bufferSize;
		this.currentBuffer = ByteBufAllocator.DEFAULT.ioBuffer(bufferSize, bufferSize);
		this.position = 0;
		this.dirty = false;
		if (checksum) {
			crc = new BufferedChecksum(new CRC32());
		} else {
			crc = null;
		}
	}

	@Override
	public void close() throws IOException {
		if (currentBuffer != null) {
			if (dirty) {
				flush();
			}
			currentBuffer.release();
			currentBuffer = null;
		}
	}


	private void flush() throws IOException {
		store.append(name, currentBuffer, 0, currentBuffer.writerIndex());
		currentBuffer.writerIndex(0);
		dirty = false;
	}

	@Override
	public long getFilePointer() {
		return position;
	}

	@Override
	public long getChecksum() {
		if (crc != null) {
			return crc.getValue();
		} else {
			throw new IllegalStateException("crc is null");
		}
	}

	@Override
	public void writeByte(byte b) throws IOException {


		if (crc != null) {
			crc.update(b);
		}
		if (currentBuffer.writerIndex() == bufferSize) {
			flush();
		}
		currentBuffer.writeByte(b);
		position++;
		dirty = true;
	}

	@Override
	public void writeBytes(byte[] b, int offset, int length) throws IOException {

		if (crc != null) {
			crc.update(b, offset, length);
		}
		int f = offset;
		int n = length;
		do {
			if (currentBuffer.writerIndex() == bufferSize) {
				flush();
			}
			int r = Math.min(bufferSize - currentBuffer.writerIndex(), n);
			currentBuffer.writeBytes(b, f, r);
			f += r;
			position += r;
			n -= r;
			dirty = true;


		} while (n != 0);
	}

	@Override
	public long ramBytesUsed() {
		return position;
	}

	@Override
	public Collection<Accountable> getChildResources() {
		return null;
	}
}