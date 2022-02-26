package it.cavallium.dbengine.lucene.directory;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Accountable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.rocksdb.RocksDBException;

/**
 * Created by wens on 16-3-10.
 */
public class RocksdbDirectory extends BaseDirectory implements Accountable {

	private static final int BUFFER_SIZE = 10 * 1024;

	protected final RocksdbFileStore store;

	protected final AtomicLong sizeInBytes = new AtomicLong();

	/** Used to generate temp file names in {@link #createTempOutput}. */
	private final AtomicLong nextTempFileCounter = new AtomicLong();

	public RocksdbDirectory(Path path) throws IOException {
		this(path, new SingleInstanceLockFactory());
	}

	/**
	 * Sole constructor.
	 *
	 */
	protected RocksdbDirectory(Path path, LockFactory lockFactory) throws IOException {
		super(lockFactory);
		store = new RocksdbFileStore(path);
	}


	public RocksdbDirectory(Path path, FSDirectory dir, IOContext context) throws IOException {
		this(path, dir, false, context);
	}

	private RocksdbDirectory(Path path, FSDirectory dir, boolean closeDir, IOContext context) throws IOException {
		this(path);
		for (String file : dir.listAll()) {
			if (!Files.isDirectory(dir.getDirectory().resolve(file))) {
				copyFrom(dir, file, file, context);
			}
		}
		if (closeDir) {
			dir.close();
		}
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
		long size = store.getSize(name);
		if (size != -1) {
			sizeInBytes.addAndGet(-size);
			store.remove(name);
		} else {
			throw new FileNotFoundException(name);
		}
	}

	/**
	 * Creates a new, empty file in the directory with the given name. Returns a stream writing this file.
	 */
	@Override
	public IndexOutput createOutput(String name, IOContext context) throws IOException {
		ensureOpen();
		try {
			if (store.contains(name)) {
				store.remove(name);
			}

			return new RocksdbOutputStream(name, store, BUFFER_SIZE, true);
		} catch (RocksDBException ex) {
			throw new IOException(ex);
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
	public void sync(Collection<String> names) {
	}

	@Override
	public void syncMetaData() {

	}

	@Override
	public void rename(String source, String dest) throws IOException {
		ensureOpen();
		try {
			if (!store.contains(source)) {
				throw new FileNotFoundException(source);
			}
			store.move(source, dest);
		} catch (RocksDBException ex) {
			throw new IOException(ex);
		}
	}

	/**
	 * Returns a stream reading an existing file.
	 */
	@Override
	public IndexInput openInput(String name, IOContext context) throws IOException {
		ensureOpen();
		try {
			if (!store.contains(name)) {
				throw new FileNotFoundException(name);
			}

			return new RocksdbInputStream(name, store, BUFFER_SIZE);
		} catch (RocksDBException ex) {
			throw new IOException(ex);
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