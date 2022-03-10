package it.cavallium.dbengine.database.disk;

import io.net5.buffer.ByteBuf;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.lmdbjava.Net5ByteBufProxy;
import org.lmdbjava.Env;
import static org.lmdbjava.EnvFlags.*;

public class LLTempLMDBEnv implements Closeable {

	private static final long TWENTY_GIBIBYTES = 20L * 1024L * 1024L * 1024L;
	public static final int MAX_DATABASES = 1024;
	private static final AtomicInteger NEXT_LMDB_ENV_ID = new AtomicInteger(0);
	private BitSet freeIds;

	private int envId;
	private Path tempDirectory;
	private Env<ByteBuf> env;
	private volatile boolean initialized;
	private volatile boolean closed;

	public LLTempLMDBEnv() {
		this.envId = NEXT_LMDB_ENV_ID.getAndIncrement();
	}

	public Env<ByteBuf> getEnv() {
		if (closed) {
			throw new IllegalStateException("Environment closed");
		}
		initializeIfPossible();
		return env;
	}

	private void initializeIfPossible() {
		if (!initialized) {
			synchronized(this) {
				if (!initialized) {
					try {
						tempDirectory = Files.createTempDirectory("lmdb");
						var envBuilder = Env.create(Net5ByteBufProxy.PROXY_NETTY)
								.setMapSize(TWENTY_GIBIBYTES)
								.setMaxDbs(MAX_DATABASES);
						//env = envBuilder.open(tempDirectory.toFile(), MDB_NOLOCK, MDB_NOSYNC, MDB_NOTLS, MDB_NORDAHEAD, MDB_WRITEMAP);
						env = envBuilder.open(tempDirectory.toFile(), MDB_NOTLS, MDB_NOSYNC, MDB_NORDAHEAD, MDB_NOMETASYNC);
						freeIds = BitSet.of(DocIdSetIterator.range(0, MAX_DATABASES), MAX_DATABASES);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
		}
	}

	public int allocateDb() {
		initializeIfPossible();
		//noinspection SynchronizeOnNonFinalField
		synchronized (freeIds) {
			var freeBit = freeIds.nextSetBit(0);
			if (freeBit == DocIdSetIterator.NO_MORE_DOCS) {
				throw new IllegalStateException("LMDB databases limit has been reached in environment "
						+ envId + ": " + MAX_DATABASES);
			}
			freeIds.clear(freeBit);
			return freeBit;
		}
	}

	public static String stringifyDbId(int bit) {
		return "$db_" + bit;
	}

	public void freeDb(int db) {
		initializeIfPossible();
		//noinspection SynchronizeOnNonFinalField
		synchronized (freeIds) {
			freeIds.set(db);
		}
	}

	@Override
	public void close() throws IOException {
		if (this.closed) {
			return;
		}
		if (!this.initialized) {
			synchronized (this) {
				closed = true;
				initialized = true;
				return;
			}
		}
		this.closed = true;
		env.close();
		//noinspection ResultOfMethodCallIgnored
		Files.walk(tempDirectory)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
	}

	public int countUsedDbs() {
		int freeIds;
		if (this.freeIds == null) {
			freeIds = MAX_DATABASES;
		} else {
			freeIds = this.freeIds.cardinality();
		}
		return MAX_DATABASES - freeIds;
	}
}
