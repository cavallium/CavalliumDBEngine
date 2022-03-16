package it.cavallium.dbengine.lucene.directory;

import com.google.common.util.concurrent.Striped;
import io.netty5.buffer.api.BufferAllocator;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.Accountable;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksdbDirectory extends BaseDirectory implements Accountable {

	private static final int BUFFER_SIZE = 10 * 1024;

	@SuppressWarnings("UnstableApiUsage")
	protected final Striped<ReadWriteLock> metaLock = Striped.readWriteLock(64);

	protected final RocksdbFileStore store;

	private final AtomicLong nextTempFileCounter = new AtomicLong();

	public RocksdbDirectory(BufferAllocator bufferAllocator, Path path, int blockSize) throws IOException {
		this(bufferAllocator, path, blockSize, new SingleInstanceLockFactory());
	}

	public RocksdbDirectory(BufferAllocator bufferAllocator,
			RocksDB db,
			Map<String, ColumnFamilyHandle> handles,
			@Nullable String name,
			int blockSize)
			throws IOException {
		this(bufferAllocator, db, handles, name, blockSize, new SingleInstanceLockFactory());
	}

	protected RocksdbDirectory(BufferAllocator bufferAllocator, Path path, int blockSize, LockFactory lockFactory)
			throws IOException {
		super(lockFactory);
		store = RocksdbFileStore.create(bufferAllocator, path, blockSize, metaLock);
	}

	protected RocksdbDirectory(BufferAllocator bufferAllocator,
			RocksDB db,
			Map<String, ColumnFamilyHandle> handles,
			@Nullable String name,
			int blockSize,
			LockFactory lockFactory) throws IOException {
		super(lockFactory);
		store = RocksdbFileStore.create(bufferAllocator, db, handles, name, blockSize, metaLock);
	}

	@Override
	public final String[] listAll() {
		ensureOpen();
		return store.listKey().toArray(String[]::new);
	}

	/**
	 * Returns the length in bytes of a file in the directory.
	 *
	 * @throws IOException if the file does not exist
	 */
	@Override
	public final long fileLength(String name) throws IOException {
		ensureOpen();
		long size = store.getSize(name);
		if (size == -1) {
			throw new FileNotFoundException(name);
		}
		return size;
	}

	/**
	 * Removes an existing file in the directory.
	 *
	 * @throws IOException if the file does not exist
	 */
	@Override
	public void deleteFile(String name) throws IOException {
		ensureOpen();
		var l = metaLock.get(name).writeLock();
		l.lock();
		try {
			long size = store.getSize(name);
			if (size != -1) {
				store.remove(name);
			} else {
				throw new FileNotFoundException(name);
			}
		} finally {
			l.unlock();
		}
	}

	/**
	 * Creates a new, empty file in the directory with the given name. Returns a stream writing this file.
	 */
	@Override
	public IndexOutput createOutput(String name, IOContext context) throws IOException {
		ensureOpen();
		var l = metaLock.get(name).writeLock();
		l.lock();
		try {
			if (store.contains(name)) {
				store.remove(name);
			}

			return new RocksdbOutputStream(name, store, BUFFER_SIZE, true);
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			l.unlock();
		}
	}
	@Override
	public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
		ensureOpen();

		String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());

		return new RocksdbOutputStream(name, store, BUFFER_SIZE, true);
	}

	/**
	 * Creates a file name for a temporary file. The name will start with {@code prefix}, end with
	 * {@code suffix} and have a reserved file extension {@code .tmp}.
	 *
	 * @see #createTempOutput(String, String, IOContext)
	 */
	protected static String getTempFileName(String prefix, String suffix, long counter) {
		return IndexFileNames.segmentFileName(
				prefix, suffix + "_" + Long.toString(counter, Character.MAX_RADIX), "tmp");
	}

	@Override
	public void sync(Collection<String> names) throws IOException {
		// System.out.println("Syncing " + names.size() + " files");
	}

	@Override
	public void syncMetaData() throws IOException {
		// System.out.println("Syncing meta");
	}

	@Override
	public void rename(String source, String dest) throws IOException {
		ensureOpen();
		var l = metaLock.bulkGet(List.of(source, dest));
		for (ReadWriteLock ll : l) {
			ll.writeLock().lock();
		}
		try {
			if (!store.contains(source)) {
				throw new FileNotFoundException(source);
			}
			store.move(source, dest);
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			for (ReadWriteLock ll : l) {
				ll.writeLock().unlock();
			}
		}
	}

	/**
	 * Returns a stream reading an existing file.
	 */
	@Override
	public IndexInput openInput(String name, IOContext context) throws IOException {
		ensureOpen();
		var l = metaLock.get(name).readLock();
		l.lock();
		try {
			if (!store.contains(name)) {
				throw new FileNotFoundException(name);
			}

			return new RocksdbInputStream(name, store, BUFFER_SIZE);
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		} finally {
			l.unlock();
		}
	}

	/**
	 * Closes the store to future operations, releasing associated memory.
	 */
	@Override
	public void close() {
		isOpen = false;
		try {
			store.close();
		} catch (IOException e) {
			throw new RuntimeException();
		}
	}

	@Override
	public Set<String> getPendingDeletions() {
		return Set.of();
	}

	@Override
	public long ramBytesUsed() {
		return 0;
	}

	@Override
	public Collection<Accountable> getChildResources() {
		return null;
	}
}