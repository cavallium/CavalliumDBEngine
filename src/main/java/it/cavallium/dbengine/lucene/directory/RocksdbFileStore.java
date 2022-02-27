package it.cavallium.dbengine.lucene.directory;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Striped;
import io.net5.buffer.ByteBuf;
import io.net5.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.lucene.store.AlreadyClosedException;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

/**
 * Created by wens on 16-3-10.
 */
public class RocksdbFileStore {

	private static final byte[] NEXT_ID_KEY = new byte[]{0x0};
	private static final String DEFAULT_COLUMN_FAMILY_STRING = new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.US_ASCII);

	static {
		RocksDB.loadLibrary();
	}

	@SuppressWarnings("UnstableApiUsage")
	private final Striped<ReadWriteLock> metaLock;

	private static final ReadOptions DEFAULT_READ_OPTS = new ReadOptions();
	private static final ReadOptions DEFAULT_IT_READ_OPTS = new ReadOptions()
			.setReadaheadSize(SizeUnit.MB)
			.setVerifyChecksums(false);
	private static final WriteOptions DEFAULT_WRITE_OPTS = new WriteOptions();
	private static final ByteBuffer EMPTY_BYTE_BUF = ByteBuffer.allocateDirect(0);

	private final RocksDB db;
	private final int blockSize;
	private final ColumnFamilyHandle headers;
	private final ColumnFamilyHandle filename;
	private final ColumnFamilyHandle size;
	private final ColumnFamilyHandle data;
	private final ConcurrentHashMap<String, Long> filenameToId = new ConcurrentHashMap<>();
	private final AtomicLong nextId;
	private final AtomicLong flushCounter = new AtomicLong();
	private volatile boolean closed;

	private RocksdbFileStore(RocksDB db,
			ColumnFamilyHandle headers,
			ColumnFamilyHandle filename,
			ColumnFamilyHandle size,
			ColumnFamilyHandle data,
			int blockSize,
			Striped<ReadWriteLock> metaLock) throws IOException {
		try {
			this.db = db;
			this.blockSize = blockSize;
			this.headers = headers;
			this.filename = filename;
			this.size = size;
			this.data = data;
			this.metaLock = metaLock;
			byte[] nextIdBytes = db.get(headers, NEXT_ID_KEY);
			if (nextIdBytes != null) {
				this.nextId = new AtomicLong(Longs.fromByteArray(nextIdBytes));
			} else {
				this.nextId = new AtomicLong();
				incFlush();
				db.put(headers, NEXT_ID_KEY, Longs.toByteArray(100));
				incFlush();
			}
		} catch (RocksDBException e) {
			throw new IOException("Failed to open RocksDB meta file store", e);
		}
	}

	private static DBOptions getDBOptions() {
		var options = new DBOptions();
		options.setWalSizeLimitMB(256);
		options.setMaxWriteBatchGroupSizeBytes(2 * SizeUnit.MB);
		options.setMaxLogFileSize(256 * SizeUnit.MB);
		options.setAtomicFlush(false);
		options.setWalRecoveryMode(WALRecoveryMode.PointInTimeRecovery);
		options.setCreateMissingColumnFamilies(true);
		options.setCreateIfMissing(true);
		options.setUnorderedWrite(true);
		options.setAvoidUnnecessaryBlockingIO(true);
		options.setSkipCheckingSstFileSizesOnDbOpen(true);
		options.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
		options.setAllowMmapReads(true);
		options.setAllowMmapWrites(true);
		options.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
		options.setDeleteObsoleteFilesPeriodMicros(Duration.ofMinutes(15).toNanos() / 1000L);
		options.setRecycleLogFileNum(10);
		return options;
	}

	private static List<ColumnFamilyDescriptor> getColumnFamilyDescriptors(@Nullable String name) {
		String headersName, filenameName, sizeName, dataName;
		if (name != null) {
			headersName = (name + "_headers");
			filenameName = (name + "_filename");
			sizeName = (name + "_size");
			dataName = (name + "_data");
		} else {
			headersName = DEFAULT_COLUMN_FAMILY_STRING;
			filenameName = "filename";
			sizeName = "size";
			dataName = "data";
		}
		return List.of(
				getColumnFamilyDescriptor(headersName),
				getColumnFamilyDescriptor(filenameName),
				getColumnFamilyDescriptor(sizeName),
				getColumnFamilyDescriptor(dataName)
		);
	}

	public static ColumnFamilyDescriptor getColumnFamilyDescriptor(String name) {
		ColumnFamilyOptions opts;
		if (name.equals(DEFAULT_COLUMN_FAMILY_STRING) || name.endsWith("_headers")) {
			opts = new ColumnFamilyOptions()
					.setCompressionType(CompressionType.NO_COMPRESSION)
					.setTargetFileSizeBase(SizeUnit.KB);
		} else if (name.endsWith("_filename")) {
			opts = new ColumnFamilyOptions()
					.setCompressionType(CompressionType.NO_COMPRESSION)
					.setTargetFileSizeBase(32L * SizeUnit.MB);
		} else if (name.endsWith("_size")) {
			opts = new ColumnFamilyOptions()
					.setCompressionType(CompressionType.NO_COMPRESSION)
					.setTargetFileSizeBase(32L * SizeUnit.MB);
		} else if (name.endsWith("_data")) {
			opts = new ColumnFamilyOptions()
					.setCompressionType(CompressionType.LZ4_COMPRESSION)
					.setTargetFileSizeBase(128L * SizeUnit.MB);
		} else {
			opts = new ColumnFamilyOptions();
		}
		return new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.US_ASCII), opts);
	}

	public static RocksdbFileStore create(RocksDB db,
			Map<String, ColumnFamilyHandle> existingHandles,
			@Nullable String name,
			int blockSize,
			Striped<ReadWriteLock> metaLock) throws IOException {
		List<ColumnFamilyDescriptor> columnFamilyDescriptors = getColumnFamilyDescriptors(name);
		try {
			List<ColumnFamilyHandle> handles = new ArrayList<>(columnFamilyDescriptors.size());
			for (ColumnFamilyDescriptor columnFamilyDescriptor : columnFamilyDescriptors) {
				var columnFamilyName = new String(columnFamilyDescriptor.getName(), StandardCharsets.US_ASCII);
				ColumnFamilyHandle columnFamilyHandle;
				if (existingHandles.containsKey(columnFamilyName)) {
					columnFamilyHandle = existingHandles.get(columnFamilyName);
				} else {
					columnFamilyHandle = db.createColumnFamily(columnFamilyDescriptor);
				}
				handles.add(columnFamilyHandle);
			}
			return new RocksdbFileStore(db,
					handles.get(0),
					handles.get(1),
					handles.get(2),
					handles.get(3),
					blockSize,
					metaLock
			);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	public static RocksdbFileStore create(Path path, int blockSize, Striped<ReadWriteLock> metaLock) throws IOException {
		try {
			DBOptions options = getDBOptions();
			List<ColumnFamilyDescriptor> descriptors = getColumnFamilyDescriptors(null);
			if (Files.notExists(path)) {
				Files.createDirectories(path);
			}
			var handles = new ArrayList<ColumnFamilyHandle>(4);
			RocksDB db = RocksDB.open(options, path.toString(), descriptors, handles);
			return new RocksdbFileStore(db,
					handles.get(0),
					handles.get(1),
					handles.get(2),
					handles.get(3),
					blockSize,
					metaLock
			);
		} catch (RocksDBException e) {
			throw new IOException("Failed to open RocksDB meta file store", e);
		}
	}

	public static RocksDBInstance createEmpty(Path path) throws IOException {
		try {
			DBOptions options = getDBOptions();
			List<ColumnFamilyDescriptor> descriptors;
			if (Files.exists(path)) {
				descriptors = RocksDB
						.listColumnFamilies(new Options(), path.toString())
						.stream()
						.map(nameBytes -> {
							var name = new String(nameBytes, StandardCharsets.US_ASCII);
							return getColumnFamilyDescriptor(name);
						})
						.toList();
			} else {
				descriptors = List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
			}
			if (Files.notExists(path)) {
				Files.createDirectories(path);
			}
			var handles = new ArrayList<ColumnFamilyHandle>(descriptors.size());
			RocksDB db = RocksDB.open(options, path.toString(), descriptors, handles);
			var handlesMap = new HashMap<String, ColumnFamilyHandle>();
			for (int i = 0; i < handles.size(); i++) {
				var name = new String(descriptors.get(i).getName(), StandardCharsets.US_ASCII);
				handlesMap.put(name, handles.get(i));
			}
			return new RocksDBInstance(db, Collections.unmodifiableMap(handlesMap));
		} catch (RocksDBException e) {
			throw new IOException("Failed to open RocksDB meta file store", e);
		}
	}

	private long getFileId(String key) throws RocksDBException, IOException {
		Long id = filenameToId.get(key);
		if (id != null) {
			return id;
		} else {
			var filenameKey = getFilenameKey(key);
			var filenameValue = getFilenameValue();
			try {
				if (db.get(filename, DEFAULT_READ_OPTS, filenameKey.nioBuffer(), filenameValue.nioBuffer(0, Long.BYTES))
						== RocksDB.NOT_FOUND) {
					throw new IOException("File not found: " + key);
				}
				filenameValue.writerIndex(Long.BYTES);
				return filenameValue.readLongLE();
			} finally {
				filenameKey.release();
				filenameValue.release();
			}
		}
	}

	@Nullable
	private Long getFileIdOrNull(String key) throws RocksDBException {
		Long id = filenameToId.get(key);
		if (id != null) {
			return id;
		} else {
			var filenameKey = getFilenameKey(key);
			var filenameValue = getFilenameValue();
			try {
				if (db.get(filename, DEFAULT_READ_OPTS, filenameKey.nioBuffer(), filenameValue.nioBuffer(0, Long.BYTES))
						== RocksDB.NOT_FOUND) {
					return null;
				}
				filenameValue.writerIndex(Long.BYTES);
				return filenameValue.readLongLE();
			} finally {
				filenameKey.release();
				filenameValue.release();
			}
		}
	}

	private boolean containsFileId(String key) throws RocksDBException {
		Long id = filenameToId.get(key);
		if (id != null) {
			return true;
		} else {
			var filenameKey = getFilenameKey(key);
			try {
				if (db.keyMayExist(filename, DEFAULT_READ_OPTS, filenameKey.nioBuffer())) {
					return db.get(filename, DEFAULT_READ_OPTS, filenameKey.nioBuffer(), EMPTY_BYTE_BUF) != RocksDB.NOT_FOUND;
				} else {
					return false;
				}
			} finally {
				filenameKey.release();
			}
		}
	}

	private void moveFileId(long id, String oldKey, String newKey) throws RocksDBException {
		var filenameOldKey = getFilenameKey(oldKey);
		var filenameNewKey = getFilenameKey(newKey);
		var filenameValue = getFilenameValue();
		filenameValue.writeLongLE(id);
		try {
			db.delete(filename, DEFAULT_WRITE_OPTS, filenameOldKey.nioBuffer());
			incFlush();
			db.put(filename, DEFAULT_WRITE_OPTS, filenameNewKey.nioBuffer(), filenameValue.nioBuffer(0, Long.BYTES));
			incFlush();
		} finally {
			filenameOldKey.release();
			filenameNewKey.release();
			filenameValue.release();
		}
	}

	private void incFlush() throws RocksDBException {
		/*
		if ((flushCounter.incrementAndGet() % 1) == 0) {
			db.flushWal(false);
		}
		 */
	}

	private long getFileIdOrAllocate(String key) throws RocksDBException {
		Long id = filenameToId.get(key);
		if (id != null) {
			return id;
		} else {
			var filenameKey = getFilenameKey(key);
			var filenameValue = getFilenameValue();
			try {
				if (db.get(filename, DEFAULT_READ_OPTS, filenameKey.nioBuffer(), filenameValue.nioBuffer(0, Long.BYTES))
						== RocksDB.NOT_FOUND) {
					filenameValue.writerIndex(0);
					filenameValue.readerIndex(0);
					var newlyAllocatedId = this.nextId.getAndIncrement();
					if (newlyAllocatedId % 100 == 99) {
						db.put(headers, new byte[] {0x00}, Longs.toByteArray(newlyAllocatedId + 1 + 100));
						incFlush();
					}
					filenameValue.writeLongLE(newlyAllocatedId);
					db.put(filename,
							DEFAULT_WRITE_OPTS,
							filenameKey.nioBuffer(),
							filenameValue.nioBuffer(0, filenameValue.writerIndex())
					);
					incFlush();
					filenameToId.put(key, newlyAllocatedId);
					return newlyAllocatedId;
				}
				filenameValue.readerIndex(0);
				filenameValue.writerIndex(Long.BYTES);
				return filenameValue.readLongLE();
			} finally {
				filenameKey.release();
				filenameValue.release();
			}
		}
	}

	private void dellocateFilename(String key) throws RocksDBException {
		var filenameKey = getFilenameKey(key);
		try {
			db.delete(filename, DEFAULT_WRITE_OPTS, filenameKey.nioBuffer());
			filenameToId.remove(key);
		} finally {
			filenameKey.release();
		}
	}

	public boolean contains(String key) throws RocksDBException, IOException {
		var l = metaLock.get(key).readLock();
		l.lock();
		try {
			ensureOpen();
			return containsFileId(key);
		} finally {
			l.unlock();
		}
	}

	private ByteBuf getMetaValueBuf() {
		return ByteBufAllocator.DEFAULT.ioBuffer(Long.BYTES, Long.BYTES);
	}

	private ByteBuf getDataValueBuf() {
		return ByteBufAllocator.DEFAULT.ioBuffer(blockSize, blockSize);
	}

	private ByteBuf getFilenameValue() {
		return ByteBufAllocator.DEFAULT.ioBuffer(Long.BYTES, Long.BYTES);
	}

	private ByteBuf getMetaKey(long id) {
		ByteBuf buf = ByteBufAllocator.DEFAULT.ioBuffer(Long.BYTES);
		buf.writeLongLE(id);
		return buf;
	}

	private ByteBuf getFilenameKey(String key) {
		ByteBuf buf = ByteBufAllocator.DEFAULT.ioBuffer(key.length());
		buf.writeCharSequence(key, StandardCharsets.US_ASCII);
		return buf;
	}

	private ByteBuf getDataKey(@Nullable ByteBuf buf, long id, int i) {
		if (buf == null) {
			buf = ByteBufAllocator.DEFAULT.buffer(Long.BYTES + Integer.BYTES);
		}
		buf.writeLongLE(id);
		buf.writeInt(i);
		return buf;
	}

	private byte[] getDataKeyByteArray(long id, int i) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[Long.BYTES + Integer.BYTES]);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		bb.putLong(id);
		bb.putInt(i);
		return bb.array();
	}


	public int load(String name, long position, byte[] buf, int offset, int len) throws IOException {
		var l = metaLock.get(name).readLock();
		l.lock();
		try {
			ensureOpen();
			Long fileId = getFileIdOrNull(name);
			if (fileId == null) {
				return -1;
			}
			long size = getSizeInternal(fileId);

			if (position >= size) {
				return -1;
			}

			if (buf.length < offset + len) {
				throw new IllegalArgumentException("len is too long");
			}

			long p = position;
			int f = offset;
			int n = len;

			ByteBuf valBuf = getDataValueBuf();
			ByteBuffer valBuffer = valBuf.nioBuffer(0, blockSize);
			boolean shouldSeekTo = true;
			try (RocksIterator it = db.newIterator(data, DEFAULT_IT_READ_OPTS)) {
				int m;
				int r;
				int i;
				do {
					m = (int) (p % (long) blockSize);
					r = Math.min(blockSize - m, n);
					i = (int) (p / (long) blockSize);

					//System.out.println("Reading block " + name + "(" + fileId + "):" + i);

					if (shouldSeekTo) {
						shouldSeekTo = false;
						ByteBuf dataKey = getDataKey(null, fileId, i);
						try {
							it.seek(dataKey.nioBuffer());
						} finally {
							dataKey.release();
						}
						if (!it.isValid()) {
							throw new IOException("Block " + name + "(" + fileId + ")" + ":" + i + " not found");
						}
					} else {
						it.next();
						if (!it.isValid()) {
							throw new IOException("Block " + name + "(" + fileId + ")" + ":" + i + " not found");
						}
						assert Arrays.equals(getDataKeyByteArray(fileId, i), it.key());
					}
					int dataRead = it.value(valBuffer);
					valBuf.writerIndex(dataRead);

					valBuf.getBytes(m, buf, f, r);

					valBuf.writerIndex(0);
					valBuf.readerIndex(0);

					p += r;
					f += r;
					n -= r;
				} while (n != 0 && p < size);

				return (int) (p - position);
			} finally {
				valBuf.release();
			}
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			l.unlock();
		}
	}

	/**
	 * @return not exist return -1
	 */
	public long getSize(String key) throws IOException {
		var l = metaLock.get(key).readLock();
		l.lock();
		try {
			ensureOpen();
			return getSizeInternal(key);
		} finally {
			l.unlock();
		}
	}

	/**
	 * @return not exist return -1
	 */
	private long getSizeInternal(String key) throws IOException {
		try {
			Long fileId = getFileIdOrNull(key);
			if (fileId == null) {
				return -1;
			}
			return getSizeInternal(fileId);
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		}
	}

	/**
	 * @return not exist return -1
	 */
	private long getSizeInternal(long fileId) throws IOException {
		try {
			ByteBuf metaKey = getMetaKey(fileId);
			ByteBuf metaData = getMetaValueBuf();
			try {
				if (db.get(size, DEFAULT_READ_OPTS, metaKey.nioBuffer(), metaData.internalNioBuffer(0, Long.BYTES))
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
		}
	}


	public void remove(String key) throws IOException {
		var l = metaLock.get(key).writeLock();
		l.lock();
		try {
			ensureOpen();
			Long fileId = getFileIdOrNull(key);
			if (fileId == null) {
				return;
			}
			long size;
			size = getSizeInternal(fileId);
			if (size == -1) {
				return;
			}
			ByteBuf dataKey = null;
			try {
				int n = (int) ((size + blockSize - 1) / blockSize);
				if (n == 1) {
					dataKey = getDataKey(dataKey, fileId, 0);
					db.delete(data, DEFAULT_WRITE_OPTS, dataKey.nioBuffer());
				} else if (n > 1) {
					var dataKey1 = getDataKeyByteArray(fileId, 0);
					var dataKey2 = getDataKeyByteArray(fileId, n - 1);
					db.deleteRange(data, DEFAULT_WRITE_OPTS, dataKey1, dataKey2);
				}
				ByteBuf metaKey = getMetaKey(fileId);
				try {
					dellocateFilename(key);
					db.delete(this.size, DEFAULT_WRITE_OPTS, metaKey.nioBuffer());
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
			l.unlock();
		}
	}


	public void clear() throws IOException {
		Lock[] locks = new Lock[metaLock.size()];
		for (int i = 0; i < metaLock.size(); i++) {
			locks[i] = metaLock.getAt(i).writeLock();
		}
		for (Lock lock : locks) {
			lock.lock();
		}
		try {
			ensureOpen();
			List<String> keySet = listKeyInternal();
			for (String key : keySet) {
				remove(key);
			}
		} finally {
			for (Lock lock : locks) {
				lock.unlock();
			}
		}
	}

	public List<String> listKey() {
		Lock[] locks = new Lock[metaLock.size()];
		for (int i = 0; i < metaLock.size(); i++) {
			locks[i] = metaLock.getAt(i).readLock();
		}
		for (Lock lock : locks) {
			lock.lock();
		}
		try {
			ensureOpen();
			return listKeyInternal();
		} finally {
			for (Lock lock : locks) {
				lock.unlock();
			}
		}
	}

	private List<String> listKeyInternal() {
		List<String> keys = new ArrayList<>();
		RocksIterator iterator = db.newIterator(filename);
		iterator.seekToFirst();
		while (iterator.isValid()) {
			keys.add(new String(iterator.key(), StandardCharsets.US_ASCII).intern());
			iterator.next();
		}
		return keys;
	}

	public void append(String name, byte[] buf, int offset, int len) throws IOException {
		var l = metaLock.get(name).writeLock();
		l.lock();
		try {
			ensureOpen();
			long size;
			long fileId;
			int f;
			int n;
			size = getSizeInternal(name);
			if (size == -1) {
				size = 0;
			}

			f = offset;
			n = len;

			fileId = getFileIdOrAllocate(name);
			ByteBuf dataKey = null;
			ByteBuf bb = getDataValueBuf();
			try {
				try (var wb = new WriteBatch(len)) {
					do {
						int m = (int) (size % (long) blockSize);
						int r = Math.min(blockSize - m, n);

						int i = (int) ((size) / (long) blockSize);
						dataKey = getDataKey(dataKey, fileId, i);
						if (m != 0) {
							int dataRead;
							if ((dataRead = db.get(data, DEFAULT_READ_OPTS, dataKey.nioBuffer(), bb.internalNioBuffer(0, blockSize)))
									== RocksDB.NOT_FOUND) {
								throw new IOException("Block " + name + "(" + fileId + "):" + i + " not found");
							}
							bb.writerIndex(dataRead);
							dataKey.readerIndex(0);
						} else {
							bb.writerIndex(0);
						}

						bb.ensureWritable(r);
						bb.setBytes(m, buf, f, r);

						wb.put(data, dataKey.nioBuffer(), bb.internalNioBuffer(0, m + r));
						incFlush();
						size += r;
						f += r;
						n -= r;

						dataKey.readerIndex(0);
						dataKey.writerIndex(0);
						bb.readerIndex(0);
						bb.writerIndex(0);
					} while (n != 0);
					db.write(DEFAULT_WRITE_OPTS, wb);
					wb.clear();
				}
			} finally {
				if (dataKey != null) {
					dataKey.release();
				}
				bb.release();
			}

			ByteBuf metaKey = getMetaKey(fileId);
			ByteBuf metaValue = getMetaValueBuf();
			try {
				metaValue.writeLongLE(size);
				db.put(this.size, DEFAULT_WRITE_OPTS, metaKey.nioBuffer(), metaValue.nioBuffer(0, Long.BYTES));
				incFlush();
			} finally {
				metaValue.release();
				metaKey.release();
			}
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			l.unlock();
		}
	}

	public void move(String source, String dest) throws IOException {
		var locks = metaLock.bulkGet(List.of(source, dest));
		for (ReadWriteLock lock : locks) {
			lock.writeLock().lock();
		}
		try {
			ensureOpen();
			long sourceFileId = getFileId(source);
			moveFileId(sourceFileId, source, dest);
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			for (ReadWriteLock lock : locks) {
				lock.writeLock().unlock();
			}
		}
	}

	private void ensureOpen() {
		if (closed) {
			throw new AlreadyClosedException("Index already closed");
		}
	}

	public void close() throws IOException {
		if (closed) {
			return;
		}
		Lock[] locks = new Lock[metaLock.size()];
		for (int i = 0; i < metaLock.size(); i++) {
			locks[i] = metaLock.getAt(i).writeLock();
		}
		for (Lock lock : locks) {
			lock.lock();
		}
		try {
			if (closed) {
				return;
			}
			db.close();
			closed = true;
		} finally {
			for (Lock lock : locks) {
				lock.unlock();
			}
		}
	}

	public void sync() throws RocksDBException {
		/*
		db.flushWal(true);
		db.flush(new FlushOptions().setAllowWriteStall(true).setWaitForFlush(true));

		 */
	}
}