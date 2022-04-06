package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.disk.LLTempHugePqEnv.getColumnOptions;

import com.google.common.primitives.Ints;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.netty5.buffer.api.BufferAllocator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class HugePqEnv implements Closeable {

	private final RocksDB db;
	private final ArrayList<ColumnFamilyHandle> defaultCfh;
	private final Int2ObjectMap<ColumnFamilyHandle> cfhs = new Int2ObjectOpenHashMap<>();

	public HugePqEnv(RocksDB db, ArrayList<ColumnFamilyHandle> defaultCfh) {
		this.db = db;
		this.defaultCfh = defaultCfh;
	}

	@Override
	public void close() throws IOException {
		for (ColumnFamilyHandle cfh : defaultCfh) {
			db.destroyColumnFamilyHandle(cfh);
			cfh.close();
		}
		try {
			db.closeE();
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	public int createColumnFamily(int name, AbstractComparator comparator) throws RocksDBException {
		var cfh = db.createColumnFamily(new ColumnFamilyDescriptor(Ints.toByteArray(name), getColumnOptions(comparator)));
		synchronized (cfhs) {
			var prev = cfhs.put(name, cfh);
			if (prev != null) {
				throw new UnsupportedOperationException("Db " + name + " already exists");
			}
			return name;
		}
	}

	public void deleteColumnFamily(int db) throws RocksDBException {
		ColumnFamilyHandle cfh;
		synchronized (cfhs) {
			cfh = cfhs.remove(db);
		}
		if (cfh != null) {
			this.db.dropColumnFamily(cfh);
			this.db.destroyColumnFamilyHandle(cfh);
			cfh.close();
		}
	}

	public StandardRocksDBColumn openDb(int hugePqId) {
		ColumnFamilyHandle cfh;
		synchronized (cfhs) {
			cfh = Objects.requireNonNull(cfhs.get(hugePqId), () -> "column " + hugePqId + " does not exist");
		}
		return new StandardRocksDBColumn(db,
				true,
				BufferAllocator.offHeapPooled(),
				db.getName(),
				cfh,
				new CompositeMeterRegistry()
		);
	}
}
