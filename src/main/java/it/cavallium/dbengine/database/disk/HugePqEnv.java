package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.disk.LLTempHugePqEnv.getColumnOptions;

import com.google.common.primitives.Ints;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.netty5.buffer.api.BufferAllocator;
import it.cavallium.dbengine.database.disk.rocksdb.RocksObj;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class HugePqEnv implements Closeable {

	private final RocksDB db;
	private final ArrayList<RocksObj<ColumnFamilyHandle>> defaultCfh;
	private final Int2ObjectMap<RocksObj<ColumnFamilyHandle>> cfhs = new Int2ObjectOpenHashMap<>();

	public HugePqEnv(RocksDB db, ArrayList<RocksObj<ColumnFamilyHandle>> defaultCfh) {
		this.db = db;
		this.defaultCfh = defaultCfh;
	}

	@Override
	public void close() throws IOException {
		for (var cfh : defaultCfh) {
			db.destroyColumnFamilyHandle(cfh.v());
			cfh.close();
		}
		try {
			db.closeE();
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	public int createColumnFamily(int name, AbstractComparator comparator) throws RocksDBException {
		var cfh = new RocksObj<>(db.createColumnFamily(new ColumnFamilyDescriptor(Ints.toByteArray(name), getColumnOptions(comparator))));
		synchronized (cfhs) {
			var prev = cfhs.put(name, cfh);
			if (prev != null) {
				throw new UnsupportedOperationException("Db " + name + " already exists");
			}
			return name;
		}
	}

	public void deleteColumnFamily(int db) throws RocksDBException {
		RocksObj<ColumnFamilyHandle> cfh;
		synchronized (cfhs) {
			cfh = cfhs.remove(db);
		}
		if (cfh != null) {
			this.db.dropColumnFamily(cfh.v());
			this.db.destroyColumnFamilyHandle(cfh.v());
			cfh.close();
		}
	}

	public StandardRocksDBColumn openDb(int hugePqId) {
		RocksObj<ColumnFamilyHandle> cfh;
		synchronized (cfhs) {
			cfh = Objects.requireNonNull(cfhs.get(hugePqId), () -> "column " + hugePqId + " does not exist");
		}
		return new StandardRocksDBColumn(db,
				true,
				BufferAllocator.offHeapPooled(),
				db.getName(),
				cfh,
				new CompositeMeterRegistry(),
				new StampedLock()
		);
	}
}
