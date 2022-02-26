package it.cavallium.dbengine.lucene.directory;

import io.net5.buffer.ByteBuf;
import io.net5.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

/**
 * Created by wens on 16-3-10.
 */
public class RocksdbFileStore {

	static {
		RocksDB.loadLibrary();
	}

	private static final int BLOCK_SIZE = 10 * 1024;
	private static final ReadOptions DEFAULT_READ_OPTS = new ReadOptions();
	private static final WriteOptions DEFAULT_WRITE_OPTS = new WriteOptions();
	private static final ByteBuffer EMPTY_BYTE_BUF = ByteBuffer.allocateDirect(0);

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private final RocksDB db;
	private final ColumnFamilyHandle meta;
	private final ColumnFamilyHandle data;

	public RocksdbFileStore(Path path) throws IOException {
		var options = new DBOptions();
		options.setCreateIfMissing(true);
		if (Files.notExists(path)) {
			Files.createDirectories(path);
		}
		options.setCreateMissingColumnFamilies(true);
		try {
			var handles = new ArrayList<ColumnFamilyHandle>(2);
			this.db = RocksDB.open(options,
					path.toString(),
					List.of(
							new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
							new ColumnFamilyDescriptor("metadata".getBytes(StandardCharsets.US_ASCII))
					),
					handles
			);
			this.meta = handles.get(0);
			this.data = handles.get(1);
		} catch (RocksDBException e) {
			throw new IOException("Failed to open RocksDB meta file store", e);
		}
	}


	public boolean contains(String key) throws RocksDBException {
		lock.readLock().lock();
		try {
			ByteBuf metaKey = getMetaKey(key);
			try {
				return db.get(meta, DEFAULT_READ_OPTS, metaKey.nioBuffer(), EMPTY_BYTE_BUF) != RocksDB.NOT_FOUND;
			} finally {
				metaKey.release();
			}
		} finally {
			lock.readLock().unlock();
		}
	}

	private ByteBuf getMetaValueBuf() {
		return ByteBufAllocator.DEFAULT.ioBuffer(Long.BYTES, Long.BYTES);
	}

	private ByteBuf getDataValueBuf() {
		return ByteBufAllocator.DEFAULT.ioBuffer(BLOCK_SIZE, BLOCK_SIZE);
	}

	private ByteBuf getMetaKey(String key) {
		ByteBuf buf = ByteBufAllocator.DEFAULT.ioBuffer(key.length());
		buf.writeCharSequence(key, StandardCharsets.US_ASCII);
		return buf;
	}

	private ByteBuf getDataKey(@Nullable ByteBuf buf, String key, int i) {
		if (buf == null) {
			buf = ByteBufAllocator.DEFAULT.buffer(key.length() + 1 + Integer.BYTES);
		}
		buf.writerIndex(0);
		buf.writeCharSequence(key, StandardCharsets.US_ASCII);
		buf.writeByte('\0');
		buf.writeIntLE(i);
		return buf;
	}


	public int load(String name, long position, byte[] buf, int offset, int len) throws IOException {
		lock.readLock().lock();
		try {
			long size = getSize(name);

			if (position >= size) {
				return -1;
			}

			if (buf.length < offset + len) {
				throw new IllegalArgumentException("len is too long");
			}

			long p = position;
			int f = offset;
			int n = len;

			ByteBuf keyBuf = null;
			ByteBuf valBuf = getDataValueBuf();
			ByteBuffer valBuffer = valBuf.nioBuffer(0, BLOCK_SIZE);
			try {
				int m;
				int r;
				int i;
				do {
					m = (int) (p % (long) BLOCK_SIZE);
					r = Math.min(BLOCK_SIZE - m, n);
					i = (int) (p / (long) BLOCK_SIZE);

					keyBuf = getDataKey(keyBuf, name, i);
					if (db.get(data, DEFAULT_READ_OPTS, keyBuf.nioBuffer(), valBuffer)
							== RocksDB.NOT_FOUND) {
						throw new IOException("Block " + name + ":" + i + " not found");
					}
					valBuf.writerIndex(BLOCK_SIZE);

					valBuf.getBytes(m, buf, f, r);

					keyBuf.writerIndex(0);
					keyBuf.readerIndex(0);
					valBuf.writerIndex(0);
					valBuf.readerIndex(0);

					p += r;
					f += r;
					n -= r;
				} while (n != 0 && p < size);

				return (int) (p - position);
			} finally {
				if (keyBuf != null) {
					keyBuf.release();
				}
				valBuf.release();
			}
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * @return not exist return -1
	 */
	public long getSize(String key) throws IOException {
		lock.readLock().lock();
		try {
			ByteBuf metaKey = getMetaKey(key);
			ByteBuf metaData = getMetaValueBuf();
			try {
				if (db.get(meta, DEFAULT_READ_OPTS, metaKey.nioBuffer(), metaData.internalNioBuffer(0, Long.BYTES))
						!= RocksDB.NOT_FOUND) {
					metaData.writerIndex(Long.BYTES);
					return metaData.readLongLE();
				} else {
					return -1;
				}
			} finally {
				metaData.release();
				metaKey.release();
			}
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			lock.readLock().unlock();
		}
	}


	public void remove(String key) throws IOException {
		lock.writeLock().lock();
		try {
			long size = getSize(key);

			if (size == -1) {
				return;
			}
			ByteBuf dataKey = null;
			try {
				int n = (int) ((size + BLOCK_SIZE - 1) / BLOCK_SIZE);
				for (int i = 0; i < n; i++) {
					dataKey = getDataKey(dataKey, key, i);
					db.delete(data, DEFAULT_WRITE_OPTS, dataKey.nioBuffer());
					dataKey.readerIndex(0);
					dataKey.writerIndex(0);
				}
				ByteBuf metaKey = getMetaKey(key);
				try {
					db.delete(meta, DEFAULT_WRITE_OPTS, metaKey.nioBuffer(0, Long.BYTES));
				} finally {
					metaKey.release();
				}
			} finally {
				if (dataKey != null) {
					dataKey.release();
				}
			}
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			lock.writeLock().unlock();
		}
	}


	public void clear() throws IOException {
		lock.writeLock().lock();
		try {
			List<String> keySet = listKey();
			for (String key : keySet) {
				remove(key);
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	public List<String> listKey() {
		List<String> keys = new ArrayList<>();
		lock.readLock().lock();
		try {
			RocksIterator iterator = db.newIterator(meta);
			iterator.seekToFirst();
			while (iterator.isValid()) {
				keys.add(new String(iterator.key()).intern());
				iterator.next();
			}
		} finally {
			lock.readLock().unlock();
		}
		return keys;
	}

	public void append(String name, byte[] buf, int offset, int len) throws IOException {
		lock.writeLock().lock();
		try {
			long size = getSize(name);
			if (size == -1) {
				size = 0;
			}

			int f = offset;
			int n = len;

			ByteBuf dataKey = null;
			ByteBuf bb = getDataValueBuf();
			try {
				do {
					int m = (int) (size % (long) BLOCK_SIZE);
					int r = Math.min(BLOCK_SIZE - m, n);

					int i = (int) ((size) / (long) BLOCK_SIZE);
					dataKey = getDataKey(dataKey, name, i);
					if (m != 0) {
						if (db.get(data, DEFAULT_READ_OPTS, dataKey.nioBuffer(), bb.internalNioBuffer(0, BLOCK_SIZE))
								== RocksDB.NOT_FOUND) {
							throw new IOException("Block " + name + ":" + i + " not found");
						}
						bb.writerIndex(BLOCK_SIZE);
						dataKey.readerIndex(0);
					} else {
						bb.writerIndex(0);
					}

					bb.ensureWritable(BLOCK_SIZE);
					bb.writerIndex(BLOCK_SIZE);
					bb.setBytes(m, buf, f, r);

					db.put(data, DEFAULT_WRITE_OPTS, dataKey.nioBuffer(), bb.nioBuffer(0, BLOCK_SIZE));
					size += r;
					f += r;
					n -= r;

					dataKey.readerIndex(0);
					dataKey.writerIndex(0);
					bb.readerIndex(0);
					bb.writerIndex(0);
				} while (n != 0);
			} finally {
				if (dataKey != null) {
					dataKey.release();
				}
				bb.release();
			}

			ByteBuf metaKey = getMetaKey(name);
			ByteBuf metaValue = getMetaValueBuf();
			try {
				metaValue.writeLongLE(size);
				db.put(meta, DEFAULT_WRITE_OPTS, metaKey.nioBuffer(), metaValue.nioBuffer(0, Long.BYTES));
			} finally {
				metaValue.release();
				metaKey.release();
			}
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			lock.writeLock().unlock();
		}
	}

	public void move(String source, String dest) throws IOException {

		lock.writeLock().lock();
		try {

			long s_size = getSize(source);
			var metaKey = getMetaKey(dest);
			var metaValue = getMetaValueBuf();
			try {
				metaValue.writeLongLE(s_size);
				db.put(meta, DEFAULT_WRITE_OPTS, metaKey.nioBuffer(), metaValue.nioBuffer(0, Long.BYTES));
			} finally {
				metaValue.release();
				metaKey.release();
			}

			int n = (int) ((s_size + BLOCK_SIZE - 1) / BLOCK_SIZE);

			ByteBuf sourceKey = null;
			ByteBuf destKey = null;
			ByteBuf dataBuf = getDataValueBuf();
			try {
				for (int i = 0; i < n; i++) {
					sourceKey = getDataKey(sourceKey, source, i);
					destKey = getDataKey(destKey, dest, i);
					var nioBuf = dataBuf.nioBuffer(0, BLOCK_SIZE);
					nioBuf.limit(BLOCK_SIZE);
					db.get(data, DEFAULT_READ_OPTS, sourceKey.nioBuffer(), nioBuf);
					nioBuf.position(BLOCK_SIZE);
					db.put(data, DEFAULT_WRITE_OPTS, destKey.nioBuffer(), nioBuf);
					sourceKey.writerIndex(0);
					sourceKey.readerIndex(0);
					destKey.writerIndex(0);
					destKey.readerIndex(0);
				}
			} finally {
				if (sourceKey != null) {
					sourceKey.release();
				}
				if (destKey != null) {
					destKey.release();
				}
				dataBuf.release();
			}
			remove(source);
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			lock.writeLock().unlock();
		}

	}

	public void close() throws IOException {
		db.close();
	}
}