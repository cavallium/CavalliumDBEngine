package it.cavallium.dbengine.lucene.directory;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Striped;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.ReadableComponent;
import io.net5.buffer.api.WritableComponent;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.lucene.store.AlreadyClosedException;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ClockCache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DirectSlice;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

public class RocksdbFileStore {

	private static final byte[] NEXT_ID_KEY = new byte[]{0x0};
	private static final String DEFAULT_COLUMN_FAMILY_STRING = new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.US_ASCII);

	static {
		RocksDB.loadLibrary();
	}

	@SuppressWarnings("UnstableApiUsage")
	private final Striped<ReadWriteLock> metaLock;
	private final ReadWriteLock[] readWriteLocks;

	private static final ReadOptions DEFAULT_READ_OPTS = new ReadOptions()
			.setVerifyChecksums(false)
			.setIgnoreRangeDeletions(true);
	private final ReadOptions itReadOpts;
	private static final WriteOptions DEFAULT_WRITE_OPTS = new WriteOptions().setDisableWAL(true);
	private static final ByteBuffer EMPTY_BYTE_BUF = ByteBuffer.allocateDirect(0);

	private final RocksDB db;
	public final BufferAllocator bufferAllocator;
	private final int blockSize;
	private final ColumnFamilyHandle headers;
	private final ColumnFamilyHandle filename;
	private final ColumnFamilyHandle size;
	private final ColumnFamilyHandle data;
	private final ConcurrentHashMap<String, Long> filenameToId = new ConcurrentHashMap<>();
	private final AtomicLong nextId;
	private final boolean closeDbOnClose;
	private volatile boolean closed;

	private RocksdbFileStore(RocksDB db,
			BufferAllocator bufferAllocator,
			ColumnFamilyHandle headers,
			ColumnFamilyHandle filename,
			ColumnFamilyHandle size,
			ColumnFamilyHandle data,
			int blockSize,
			Striped<ReadWriteLock> metaLock,
			boolean closeDbOnClose) throws IOException {
		try {
			this.db = db;
			this.bufferAllocator = bufferAllocator;
			this.closeDbOnClose = closeDbOnClose;
			this.blockSize = blockSize;
			this.headers = headers;
			this.filename = filename;
			this.size = size;
			this.data = data;
			this.metaLock = metaLock;
			ReadWriteLock[] locks = new ReadWriteLock[metaLock.size()];
			for (int i = 0; i < metaLock.size(); i++) {
				locks[i] = metaLock.getAt(i);
			}
			this.readWriteLocks = locks;
			byte[] nextIdBytes = db.get(headers, NEXT_ID_KEY);
			if (nextIdBytes != null) {
				this.nextId = new AtomicLong(Longs.fromByteArray(nextIdBytes));
			} else {
				this.nextId = new AtomicLong();
				incFlush();
				db.put(headers, NEXT_ID_KEY, Longs.toByteArray(100));
				incFlush();
			}
			this.itReadOpts = new ReadOptions()
					.setReadaheadSize(blockSize * 4L)
					.setVerifyChecksums(false)
					.setIgnoreRangeDeletions(true);
		} catch (RocksDBException e) {
			throw new IOException("Failed to open RocksDB meta file store", e);
		}
	}

	private static ByteBuffer readableNioBuffer(Buffer buffer) {
		assert buffer.countReadableComponents() == 1 : "Readable components count: " + buffer.countReadableComponents();
		return ((ReadableComponent) buffer).readableBuffer();
	}

	private static ByteBuffer writableNioBuffer(Buffer buffer, int newWriterOffset) {
		assert buffer.countWritableComponents() == 1 : "Writable components count: " + buffer.countWritableComponents();
		buffer.writerOffset(0).ensureWritable(newWriterOffset);
		var byteBuf = ((WritableComponent) buffer).writableBuffer();
		buffer.writerOffset(newWriterOffset);
		assert buffer.capacity() >= newWriterOffset : "Returned capacity " + buffer.capacity() + " < " + newWriterOffset;
		return byteBuf;
	}

	private static DBOptions getDBOptions() {
		var options = new DBOptions();
		options.setParanoidChecks(false);
		options.setWalSizeLimitMB(256);
		options.setMaxWriteBatchGroupSizeBytes(2 * SizeUnit.MB);
		//options.setAtomicFlush(false);
		options.setWalRecoveryMode(WALRecoveryMode.PointInTimeRecovery);
		options.setCreateMissingColumnFamilies(true);
		options.setCreateIfMissing(true);
		//options.setUnorderedWrite(true);
		options.setAvoidUnnecessaryBlockingIO(true);
		options.setSkipCheckingSstFileSizesOnDbOpen(true);
		options.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
		//options.setAllowMmapReads(true);
		//options.setAllowMmapWrites(true);
		options.setUseDirectReads(true);
		options.setUseDirectIoForFlushAndCompaction(true);
		options.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
		options.setDeleteObsoleteFilesPeriodMicros(Duration.ofMinutes(15).toNanos() / 1000L);
		options.setRowCache(new ClockCache(512 * 1024 * 1024L));
		options.setMaxOpenFiles(500);
		return options;
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

	public static RocksdbFileStore create(BufferAllocator bufferAllocator,
			RocksDB db,
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
					bufferAllocator,
					handles.get(0),
					handles.get(1),
					handles.get(2),
					handles.get(3),
					blockSize,
					metaLock,
					false
			);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	public static RocksdbFileStore create(BufferAllocator bufferAllocator,
			Path path,
			int blockSize,
			Striped<ReadWriteLock> metaLock) throws IOException {
		try {
			DBOptions options = getDBOptions();
			List<ColumnFamilyDescriptor> descriptors = getColumnFamilyDescriptors(null);
			if (Files.notExists(path)) {
				Files.createDirectories(path);
			}
			var handles = new ArrayList<ColumnFamilyHandle>(4);
			RocksDB db = RocksDB.open(options, path.toString(), descriptors, handles);
			return new RocksdbFileStore(db,
					bufferAllocator,
					handles.get(0),
					handles.get(1),
					handles.get(2),
					handles.get(3),
					blockSize,
					metaLock,
					true
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
			try (var filenameKey = getFilenameKey(key); var filenameValue = getFilenameValue()) {
				if (db.get(filename, DEFAULT_READ_OPTS, readableNioBuffer(filenameKey), writableNioBuffer(filenameValue, Long.BYTES))
						== RocksDB.NOT_FOUND) {
					throw new IOException("File not found: " + key);
				}
				filenameValue.writerOffset(Long.BYTES);
				return filenameValue.readLong();
			}
		}
	}

	@Nullable
	private Long getFileIdOrNull(String key) throws RocksDBException {
		Long id = filenameToId.get(key);
		if (id != null) {
			return id;
		} else {
			try (var filenameKey = getFilenameKey(key); var filenameValue = getFilenameValue()) {
				if (db.get(filename, DEFAULT_READ_OPTS, readableNioBuffer(filenameKey), writableNioBuffer(filenameValue, Long.BYTES))
						== RocksDB.NOT_FOUND) {
					return null;
				}
				filenameValue.writerOffset(Long.BYTES);
				return filenameValue.readLong();
			}
		}
	}

	private boolean containsFileId(String key) throws RocksDBException {
		Long id = filenameToId.get(key);
		if (id != null) {
			return true;
		} else {
			try (var filenameKey = getFilenameKey(key)) {
				if (db.keyMayExist(filename, DEFAULT_READ_OPTS, readableNioBuffer(filenameKey))) {
					return db.get(filename, DEFAULT_READ_OPTS, readableNioBuffer(filenameKey), EMPTY_BYTE_BUF) != RocksDB.NOT_FOUND;
				} else {
					return false;
				}
			}
		}
	}

	private void moveFileId(long id, String oldKey, String newKey) throws RocksDBException {
		var filenameValue = getFilenameValue();
		filenameValue.writeLong(id);
		try (var filenameOldKey = getFilenameKey(oldKey); var filenameNewKey = getFilenameKey(newKey); filenameValue) {
			db.delete(filename, DEFAULT_WRITE_OPTS, readableNioBuffer(filenameOldKey));
			incFlush();
			db.put(filename, DEFAULT_WRITE_OPTS, readableNioBuffer(filenameNewKey), readableNioBuffer(filenameValue));
			incFlush();
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
			try (var filenameKey = getFilenameKey(key); var filenameValue = getFilenameValue()) {
				if (db.get(filename, DEFAULT_READ_OPTS, readableNioBuffer(filenameKey),
						writableNioBuffer(filenameValue, Long.BYTES))
						== RocksDB.NOT_FOUND) {
					filenameValue.writerOffset(0);
					filenameValue.readerOffset(0);
					var newlyAllocatedId = this.nextId.getAndIncrement();
					if (newlyAllocatedId % 100 == 99) {
						db.put(headers, new byte[]{0x00}, Longs.toByteArray(newlyAllocatedId + 1 + 100));
						incFlush();
					}
					filenameValue.writeLong(newlyAllocatedId);
					db.put(filename,
							DEFAULT_WRITE_OPTS,
							readableNioBuffer(filenameKey),
							readableNioBuffer(filenameValue)
					);
					incFlush();
					filenameToId.put(key, newlyAllocatedId);
					return newlyAllocatedId;
				}
				filenameValue.readerOffset(0);
				filenameValue.writerOffset(Long.BYTES);
				return filenameValue.readLong();
			}
		}
	}

	private void dellocateFilename(String key) throws RocksDBException {
		try (var filenameKey = getFilenameKey(key)) {
			db.delete(filename, DEFAULT_WRITE_OPTS, readableNioBuffer(filenameKey));
			filenameToId.remove(key);
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

	private Buffer getMetaValueBuf() {
		return bufferAllocator.allocate(Long.BYTES);
	}

	private Buffer getDataValueBuf() {
		return bufferAllocator.allocate(blockSize);
	}

	private Buffer getFilenameValue() {
		return bufferAllocator.allocate(Long.BYTES);
	}

	private Buffer getMetaKey(long id) {
		Buffer buf = bufferAllocator.allocate(Long.BYTES);
		buf.writeLong(id);
		return buf;
	}

	private Buffer getFilenameKey(String key) {
		Buffer buf = bufferAllocator.allocate(key.length());
		buf.writeCharSequence(key, StandardCharsets.US_ASCII);
		return buf;
	}

	private Buffer getDataKey(@Nullable Buffer buf, long id, int i) {
		if (buf == null) {
			buf = bufferAllocator.allocate(Long.BYTES + Integer.BYTES);
		}
		buf.writeLong(id);
		buf.writeInt(i);
		return buf;
	}

	private Buffer getDataKeyPrefix(long id) {
		var buf = bufferAllocator.allocate(Long.BYTES);
		buf.writeLong(id);
		return buf;
	}

	private byte[] getDataKeyByteArray(long id, int i) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[Long.BYTES + Integer.BYTES]);
		bb.putLong(id);
		bb.putInt(i);
		return bb.array();
	}


	public int load(String name, long position, Buffer buf, int offset, int len) throws IOException {
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

			if (buf.capacity() < offset + len) {
				throw new IllegalArgumentException("len is too long");
			}

			long p = position;
			int f = offset;
			int n = len;

			Buffer valBuf = getDataValueBuf();
			try (valBuf) {
				ByteBuffer valBuffer = writableNioBuffer(valBuf, blockSize);
				boolean shouldSeekTo = true;
				try (var ro = new ReadOptions(itReadOpts)) {
					ro.setIgnoreRangeDeletions(true);
					try (Buffer fileIdPrefix = getDataKeyPrefix(fileId)) {
						try (var lb = new DirectSlice(readableNioBuffer(fileIdPrefix), Long.BYTES)) {
							ro.setIterateLowerBound(lb);
							ro.setPrefixSameAsStart(true);
							try (RocksIterator it = db.newIterator(data, itReadOpts)) {
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
										try (Buffer dataKey = getDataKey(null, fileId, i)) {
											it.seek(readableNioBuffer(dataKey));
										}
										if (!it.isValid()) {
											throw new IOException("Block " + name + "(" + fileId + ")" + ":" + i + " not found");
										}
									} else {
										it.next();
										if (!it.isValid()) {
											throw new IOException("Block " + name + "(" + fileId + ")" + ":" + i + " not found");
										}
									}
									assert Arrays.equals(getDataKeyByteArray(fileId, i), it.key());
									int dataRead = it.value(valBuffer);
									valBuf.writerOffset(dataRead);

									valBuf.copyInto(m, buf, f, r);

									valBuf.writerOffset(0);
									valBuf.readerOffset(0);

									p += r;
									f += r;
									n -= r;
								} while (n != 0 && p < size);

								return (int) (p - position);
							}
						}
					}
				}
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
			try (Buffer metaKey = getMetaKey(fileId); Buffer metaData = getMetaValueBuf()) {
				if (db.get(size, DEFAULT_READ_OPTS, readableNioBuffer(metaKey), writableNioBuffer(metaData, Long.BYTES))
						!= RocksDB.NOT_FOUND) {
					metaData.writerOffset(Long.BYTES);
					return metaData.readLong();
				} else {
					return -1;
				}
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
			Buffer dataKey = null;
			try {
				int n = (int) ((size + blockSize - 1) / blockSize);
				if (n == 1) {
					dataKey = getDataKey(dataKey, fileId, 0);
					db.delete(data, DEFAULT_WRITE_OPTS, readableNioBuffer(dataKey));
				} else if (n > 1) {
					var dataKey1 = getDataKeyByteArray(fileId, 0);
					var dataKey2 = getDataKeyByteArray(fileId, n - 1);
					db.deleteRange(data, DEFAULT_WRITE_OPTS, dataKey1, dataKey2);
				}
				try (Buffer metaKey = getMetaKey(fileId)) {
					dellocateFilename(key);
					db.delete(this.size, DEFAULT_WRITE_OPTS, readableNioBuffer(metaKey));
				}
			} finally {
				if (dataKey != null) {
					dataKey.close();
				}
			}
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			l.unlock();
		}
	}


	public void clear() throws IOException {
		for (var lock : readWriteLocks) {
			lock.writeLock().lock();
		}
		try {
			ensureOpen();
			List<String> keySet = listKeyInternal();
			for (String key : keySet) {
				remove(key);
			}
		} finally {
			for (var lock : readWriteLocks) {
				lock.writeLock().unlock();
			}
		}
	}

	public List<String> listKey() {
		ensureOpen();
		for (var lock : readWriteLocks) {
			lock.readLock().lock();
		}
		try {
			ensureOpen();
			return listKeyInternal();
		} finally {
			for (var lock : readWriteLocks) {
				lock.readLock().unlock();
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

	public void append(String name, Buffer buf, int offset, int len) throws IOException {
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
			Buffer dataKey = null;
			Buffer bb = getDataValueBuf();
			try {
				do {
					int m = (int) (size % (long) blockSize);
					int r = Math.min(blockSize - m, n);

					int i = (int) ((size) / (long) blockSize);
					dataKey = getDataKey(dataKey, fileId, i);
					if (m != 0) {
						int dataRead;
						if ((dataRead = db.get(data,
								DEFAULT_READ_OPTS,
								readableNioBuffer(dataKey),
								writableNioBuffer(bb, blockSize)
						)) == RocksDB.NOT_FOUND) {
							throw new IOException("Block " + name + "(" + fileId + "):" + i + " not found");
						}
						bb.writerOffset(dataRead);
						dataKey.readerOffset(0);
					} else {
						bb.writerOffset(0);
					}

					bb.ensureWritable(r);
					buf.copyInto(f, bb, m, r);

					var bbBuf = writableNioBuffer(bb, m + r);

					assert bbBuf.capacity() >= m + r : bbBuf.capacity() + " < " + (m + r);
					assert bbBuf.position() == 0;
					bbBuf.limit(m + r);
					assert bbBuf.limit() == m + r;

					db.put(data, DEFAULT_WRITE_OPTS, readableNioBuffer(dataKey), bbBuf);
					incFlush();
					size += r;
					f += r;
					n -= r;

					dataKey.readerOffset(0);
					dataKey.writerOffset(0);
					bb.readerOffset(0);
					bb.writerOffset(0);
				} while (n != 0);
			} finally {
				if (dataKey != null) {
					dataKey.close();
				}
				bb.close();
			}

			try (Buffer metaKey = getMetaKey(fileId); Buffer metaValue = getMetaValueBuf()) {
				metaValue.writeLong(size);
				db.put(this.size, DEFAULT_WRITE_OPTS, readableNioBuffer(metaKey), readableNioBuffer(metaValue));
				incFlush();
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
		for (var lock : readWriteLocks) {
			lock.writeLock().lock();
		}
		try {
			if (closed) {
				return;
			}
			closed = true;
			if (closeDbOnClose) {
				try {
					db.closeE();
				} catch (RocksDBException e) {
					throw new IOException(e);
				}
			}
		} finally {
			for (var lock : readWriteLocks) {
				lock.writeLock().unlock();
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