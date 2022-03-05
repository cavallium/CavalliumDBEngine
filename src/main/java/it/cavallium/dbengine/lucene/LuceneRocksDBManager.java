package it.cavallium.dbengine.lucene;

import it.cavallium.dbengine.lucene.directory.RocksDBInstance;
import it.cavallium.dbengine.lucene.directory.RocksdbFileStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LuceneRocksDBManager {

	private static final Logger LOG = LogManager.getLogger(LuceneRocksDBManager.class);
	private final List<Map.Entry<Path, RocksDBInstance>> dbs = new ArrayList<>();

	public synchronized RocksDBInstance getOrCreate(Path path) {
		try {
			for (var entry : dbs) {
				if (Files.isSameFile(path, entry.getKey())) {
					return entry.getValue();
				}
			}
			RocksDBInstance db = RocksdbFileStore.createEmpty(path);
			dbs.add(Map.entry(path, db));
			return db;
		} catch (IOException ex) {
			throw new UnsupportedOperationException("Can't load RocksDB database at path: " + path, ex);
		}
	}

	public synchronized void closeAll() {
		for (Entry<Path, RocksDBInstance> db : dbs) {
			try {
				db.getValue().db().closeE();
			} catch (Throwable ex) {
				LOG.error("Failed to close lucene RocksDB database", ex);
			}
		}
		dbs.clear();
	}
}
