package it.cavallium.dbengine;

import it.cavallium.dbengine.database.disk.RocksLog4jLogger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.PersistentCache;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

public class TestPersistentCache {

	public static Logger LOG = LogManager.getLogger(TestPersistentCache.class);

	@Test
	public void test() throws RocksDBException, IOException {
		var dbDir = Files.createTempDirectory("test-");
		var persistentCacheDir = dbDir.resolve("persistent-cache");
		LOG.info("Persistent cache directory: {}", persistentCacheDir);
		Files.createDirectories(persistentCacheDir);
		try (var dbOpts = new DBOptions(); var persistentCache = new PersistentCache(Env.getDefault(),
				persistentCacheDir.toString(),
				2 * SizeUnit.GB,
				new RocksLog4jLogger(dbOpts, LOG),
				false
		)) {
			dbOpts.setCreateIfMissing(true);
			dbOpts.setCreateMissingColumnFamilies(true);
			dbOpts.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
			var col1Options = new ColumnFamilyOptions();
			var tableConfig = new BlockBasedTableConfig();
			tableConfig.setPersistentCache(persistentCache);
			col1Options.setTableFormatConfig(tableConfig);
			var col1Descriptor = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, col1Options);
			var col2Options = new ColumnFamilyOptions();
			col2Options.setTableFormatConfig(tableConfig);
			var col2Descriptor = new ColumnFamilyDescriptor("col2".getBytes(StandardCharsets.UTF_8), col2Options);
			var handles = new ArrayList<ColumnFamilyHandle>(2);
			try (var db = RocksDB.open(dbOpts, dbDir.toString(), List.of(col1Descriptor, col2Descriptor), handles)) {
				var cfh1 = handles.get(0);
				var cfh2 = handles.get(1);
				try (var writeOptions = new WriteOptions()) {
					writeOptions.setDisableWAL(true);
					db.put(cfh1, writeOptions, "col1-key1".getBytes(), "col1-key1-value".getBytes());
					for (int i = 0; i < (2 << 16); i++) {
						db.put(cfh2, writeOptions, ("col2-key" + i).getBytes(), ("col2-key" + i  + "-value").getBytes());
					}
				}
				try (var flushOptions = new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true)) {
					db.flush(flushOptions);
				}
				try (var readOptions = new ReadOptions()) {
					var col1Key1Value = new String(db.get(cfh1, readOptions, "col1-key1".getBytes()), StandardCharsets.UTF_8);
					var col2Key1Value = new String(db.get(cfh2, readOptions, "col2-key1".getBytes()), StandardCharsets.UTF_8);
					Assertions.assertEquals("col1-key1-value", col1Key1Value);
					Assertions.assertEquals("col2-key1-value", col2Key1Value);
				}
			}
		} finally {
			try (var fileWalker = Files.walk(dbDir)) {
				fileWalker.sorted(Comparator.reverseOrder()).forEach(file -> {
					try {
						Files.delete(file);
					} catch (IOException ex) {
						throw new CompletionException(ex);
					}
				});
			}
		}
	}
}
