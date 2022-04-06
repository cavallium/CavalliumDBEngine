package it.cavallium.dbengine.database.disk;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.rocksdb.AbstractComparator;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ChecksumType;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class LLTempHugePqEnv implements Closeable {

	private Path tempDirectory;
	private AtomicInteger nextColumnName;
	private HugePqEnv env;
	private volatile boolean initialized;
	private volatile boolean closed;

	public LLTempHugePqEnv() {
	}

	public HugePqEnv getEnv() {
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
						tempDirectory = Files.createTempDirectory("huge-pq");
						var opts = new DBOptions();
						opts.setCreateIfMissing(true);
						opts.setAtomicFlush(false);
						opts.optimizeForSmallDb();
						opts.setParanoidChecks(false);
						opts.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
						opts.setMaxOpenFiles(-1);
						opts.setUseFsync(false);
						opts.setUnorderedWrite(true);
						opts.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);

						var cfh = new ArrayList<ColumnFamilyHandle>();
						nextColumnName = new AtomicInteger(0);
						env = new HugePqEnv(RocksDB.open(opts,
								tempDirectory.toString(),
								List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, getColumnOptions(null))),
								cfh
						), cfh);
						initialized = true;
					} catch (RocksDBException | IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
		}
	}

	static ColumnFamilyOptions getColumnOptions(AbstractComparator comparator) {
		var opts = new ColumnFamilyOptions()
				.setOptimizeFiltersForHits(true)
				.setParanoidFileChecks(false)
				.optimizeLevelStyleCompaction()
				.setLevelCompactionDynamicLevelBytes(true)
				.setTableFormatConfig(new BlockBasedTableConfig()
						.setOptimizeFiltersForMemory(true)
						.setChecksumType(ChecksumType.kNoChecksum));
		if (comparator != null) {
			opts.setComparator(comparator);
		}
		return opts;
	}

	public int allocateDb(AbstractComparator comparator) {
		initializeIfPossible();
		try {
			return env.createColumnFamily(nextColumnName.getAndIncrement(), comparator);
		} catch (RocksDBException e) {
			throw new IllegalStateException(e);
		}
	}

	public void freeDb(int db) {
		initializeIfPossible();
		try {
			env.deleteColumnFamily(db);
		} catch (RocksDBException e) {
			throw new IllegalStateException(e);
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
}
